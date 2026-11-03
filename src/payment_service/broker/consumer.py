"""Единственный consumer: шлюз -> статус в БД -> webhook. Retry и DLQ на уровне сообщений."""

import logging
from datetime import UTC, datetime

from faststream import AckPolicy
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker.topology import (
    ATTEMPT_HEADER,
    LAST_ERROR_HEADER,
    NEW_QUEUE,
    PAYMENTS_EXCHANGE,
    RETRY_EXCHANGE,
    retry_routing_key,
)
from payment_service.config import Settings
from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus
from payment_service.domain.schemas import PaymentCreatedEvent, WebhookPayload
from payment_service.services.gateway import PaymentGatewayEmulator
from payment_service.services.webhook import WebhookSender

log = logging.getLogger(__name__)


class PaymentProcessor:
    """Бизнес-логика обработки одного события. Идемпотентна: каждый шаг
    проверяет, не выполнен ли он уже, поэтому повторная доставка безопасна."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGatewayEmulator,
        webhook: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, event: PaymentCreatedEvent, attempt: int) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, event.payment_id)
            if payment is None:
                # платёж и событие пишутся в одной транзакции, так что это битое сообщение
                log.error("payment %s not found, rejecting to DLQ", event.payment_id)
                raise RejectMessage()
            # отпускаем соединение с БД на время вызова шлюза и webhook'а
            await session.commit()

            if payment.status.is_terminal:
                log.info("payment %s already %s, skip gateway", payment.id, payment.status)
            else:
                result = await self._gateway.charge(payment.id, payment.amount, payment.currency)
                # условный UPDATE: если то же событие параллельно обрабатывает другой handler
                # (redelivery после обрыва соединения), результат записывает только первый
                written = await session.execute(
                    update(Payment)
                    .where(Payment.id == payment.id, Payment.status == PaymentStatus.PENDING)
                    .values(
                        status=PaymentStatus.SUCCEEDED if result.success else PaymentStatus.FAILED,
                        failure_reason=result.reason,
                        processed_at=datetime.now(UTC),
                    )
                )
                await session.commit()
                if written.rowcount == 0:
                    log.warning(
                        "payment %s was processed concurrently, keeping stored result", payment.id
                    )
                await session.refresh(payment)
                log.info("payment %s processed: %s", payment.id, payment.status)

            if payment.webhook_delivered_at is not None:
                log.info("payment %s webhook already delivered, skip", payment.id)
                return

            await self._webhook.send(
                payment.webhook_url,
                WebhookPayload.model_validate(payment),
                event_id=str(event.event_id),
                attempt=attempt,
            )
            payment.webhook_delivered_at = datetime.now(UTC)
            await session.commit()


def register_consumer(
    broker: RabbitBroker, processor: PaymentProcessor, settings: Settings
) -> None:
    @broker.subscriber(
        NEW_QUEUE,
        PAYMENTS_EXCHANGE,
        channel=Channel(prefetch_count=settings.consumer_prefetch),
        # необработанное исключение (в том числе ошибка валидации тела)
        # -> reject(requeue=False) -> DLX -> payments.dlq
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment_created(event: PaymentCreatedEvent, msg: RabbitMessage) -> None:
        attempt = int(msg.headers.get(ATTEMPT_HEADER, 1))
        log.info(
            "processing payment %s (attempt %s/%s)",
            event.payment_id,
            attempt,
            settings.max_attempts,
        )
        try:
            await processor.process(event, attempt)
        except RejectMessage:
            raise
        except Exception as exc:
            if attempt >= settings.max_attempts:
                log.error(
                    "payment %s: attempt %s/%s failed (%s), rejecting to DLQ",
                    event.payment_id,
                    attempt,
                    settings.max_attempts,
                    exc,
                )
                raise
            next_attempt = attempt + 1
            delay = settings.retry_delay(next_attempt)
            log.warning(
                "payment %s: attempt %s failed (%s), retry #%s in %.1fs",
                event.payment_id,
                attempt,
                exc,
                next_attempt,
                delay,
            )
            try:
                await broker.publish(
                    event.model_dump(mode="json"),
                    exchange=RETRY_EXCHANGE,
                    routing_key=retry_routing_key(next_attempt),
                    message_id=msg.message_id,
                    persist=True,
                    headers={
                        ATTEMPT_HEADER: next_attempt,
                        LAST_ERROR_HEADER: f"{exc.__class__.__name__}: {exc}"[:500],
                    },
                )
            except Exception as publish_exc:
                # не удалось положить копию в retry: вернуть оригинал в очередь, а не в DLQ
                log.error(
                    "payment %s: retry publish failed (%s), nack", event.payment_id, publish_exc
                )
                raise NackMessage(requeue=True) from publish_exc
            # исходное сообщение подтверждаем: его копия уже лежит в retry-очереди
