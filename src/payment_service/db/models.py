import enum
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from payment_service.db.base import Base
from payment_service.domain.enums import Currency, PaymentStatus


def _values(enum_cls: type[enum.Enum]) -> list[str]:
    """Хранить в БД значения enum ('pending'), а не имена ('PENDING')."""
    return [member.value for member in enum_cls]


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="currency_allowed"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="status_allowed"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, native_enum=False, length=3, values_callable=_values),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    # имя атрибута metadata занято в DeclarativeBase, поэтому metadata_
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, default=dict, server_default="{}"
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, native_enum=False, length=16, values_callable=_values),
        nullable=False,
        default=PaymentStatus.PENDING,
        index=True,
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    # sha256 от тела запроса: тот же ключ с другим телом -> 422, а не тихий возврат чужого платежа
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(Text, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class OutboxMessage(Base):
    """Transactional outbox: событие пишется в одной транзакции с платежом,
    отдельный relay публикует его в RabbitMQ и помечает published_at."""

    __tablename__ = "outbox"
    __table_args__ = (
        # relay выбирает только неопубликованные, индекс частичный
        Index(
            "ix_outbox_unpublished",
            "created_at",
            postgresql_where="published_at IS NULL",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    routing_key: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
