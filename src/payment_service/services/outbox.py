"""Outbox relay: читает неопубликованные события и публикует их в RabbitMQ.

Выборка с FOR UPDATE SKIP LOCKED, поэтому несколько экземпляров relay не мешают
друг другу. Публикация идёт с publisher confirms: published_at ставится только
после подтверждения брокером. Если процесс упадёт между confirm и commit, событие
опубликуется повторно (at-least-once), consumer это переживает за счёт идемпотентности.
"""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker.topology import PAYMENTS_EXCHANGE
from payment_service.db.models import OutboxMessage

log = logging.getLogger(__name__)


class OutboxRelay:
    MAX_BACKOFF = 10.0

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        poll_interval: float,
        batch_size: int,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._poll_interval = poll_interval
        self._batch_size = batch_size

    async def run(self, stop: asyncio.Event) -> None:
        log.info(
            "outbox relay started (poll=%.2fs, batch=%s)", self._poll_interval, self._batch_size
        )
        failures = 0
        while not stop.is_set():
            try:
                published, failed = await self.relay_once()
            except Exception:
                log.exception("outbox relay iteration failed")
                published, failed = 0, 1

            if failed:
                # брокер недоступен: ждём с экспоненциальным backoff, но не дольше MAX_BACKOFF
                failures += 1
                delay = min(self._poll_interval * 2**failures, self.MAX_BACKOFF)
            else:
                failures = 0
                # очередь пуста - ждём poll_interval; иначе сразу берём следующую пачку
                delay = self._poll_interval if published < self._batch_size else 0
            if delay:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
        log.info("outbox relay stopped")

    async def relay_once(self) -> tuple[int, int]:
        """Возвращает (опубликовано, не удалось). В outbox.attempts считаются только неудачи."""
        published = failed = 0
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                select(OutboxMessage)
                .where(OutboxMessage.published_at.is_(None))
                .order_by(OutboxMessage.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            for message in result.scalars():
                try:
                    await self._broker.publish(
                        message.payload,
                        exchange=PAYMENTS_EXCHANGE,
                        routing_key=message.routing_key,
                        message_id=str(message.id),
                        message_type=message.event_type,
                        persist=True,
                    )
                except Exception as exc:
                    message.attempts += 1
                    message.last_error = f"{exc.__class__.__name__}: {exc}"[:500]
                    log.warning("outbox publish failed id=%s: %s", message.id, message.last_error)
                    failed += 1
                    break  # брокер недоступен для всей пачки, остальные строки возьмём позже
                message.published_at = datetime.now(UTC)
                published += 1
        if published:
            log.info("outbox relay published %s event(s)", published)
        return published, failed
