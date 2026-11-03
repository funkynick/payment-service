"""Создание и чтение платежей. Создание = платёж + outbox-событие в одной транзакции."""

import hashlib
import json
import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.broker.topology import NEW_ROUTING_KEY
from payment_service.db.models import OutboxMessage, Payment
from payment_service.domain.schemas import PaymentCreate, PaymentCreatedEvent


class IdempotencyConflict(Exception):
    """Тот же Idempotency-Key пришёл с другим телом запроса."""


def request_fingerprint(data: PaymentCreate) -> str:
    """sha256 канонического JSON тела: порядок ключей и формат чисел не влияют."""
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def get_by_idempotency_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> tuple[Payment, bool]:
    """Возвращает (платёж, created).

    created=False означает повтор запроса с тем же ключом и тем же телом:
    отдаём уже существующий платёж, ничего не создавая.
    Гонка двух одновременных запросов с одним ключом разрешается
    уникальным индексом: проигравший ловит IntegrityError и перечитывает победителя.
    """
    fingerprint = request_fingerprint(data)

    existing = await get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return _check_replay(existing, fingerprint, idempotency_key), False

    payment = Payment(
        id=uuid.uuid4(),  # нужен до flush: кладём в событие
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata_=data.metadata,
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
        webhook_url=str(data.webhook_url),
    )
    event = PaymentCreatedEvent(
        event_id=uuid.uuid4(), payment_id=payment.id, occurred_at=datetime.now(UTC)
    )
    outbox = OutboxMessage(
        id=event.event_id,
        aggregate_type="payment",
        aggregate_id=payment.id,
        event_type="payment.created",
        routing_key=NEW_ROUTING_KEY,
        payload=event.model_dump(mode="json"),
    )
    session.add_all([payment, outbox])
    try:
        await session.commit()  # INSERT ... RETURNING подтягивает created_at, refresh не нужен
    except IntegrityError:
        await session.rollback()
        existing = await get_by_idempotency_key(session, idempotency_key)
        if existing is None:
            raise
        return _check_replay(existing, fingerprint, idempotency_key), False

    return payment, True


def _check_replay(existing: Payment, fingerprint: str, key: str) -> Payment:
    if existing.request_hash != fingerprint:
        raise IdempotencyConflict(
            f"Idempotency-Key {key!r} already used with a different request body"
        )
    return existing
