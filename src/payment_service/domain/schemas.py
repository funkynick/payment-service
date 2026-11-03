import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from payment_service.domain.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Тело POST /api/v1/payments."""

    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"])
    currency: Currency
    description: str = Field(min_length=1, max_length=255, examples=["Order #42"])
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(examples=["http://webhook-sink:8080/webhook"])

    @field_validator("amount")
    @classmethod
    def _normalize_amount(cls, value: Decimal) -> Decimal:
        """100.5 и 100.50 - одна и та же сумма: важно для отпечатка идемпотентности и БД."""
        return value.quantize(Decimal("0.01"))


class _FromPayment(BaseModel):
    """Схемы, собираемые из ORM-объекта Payment через model_validate(payment).
    Атрибут модели называется metadata_ (имя metadata занято в DeclarativeBase)."""

    model_config = ConfigDict(from_attributes=True)


class PaymentAccepted(_FromPayment):
    """Ответ 202 на создание платежа."""

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentRead(_FromPayment):
    """Детальная информация о платеже."""

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    failure_reason: str | None
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    updated_at: datetime


class PaymentCreatedEvent(BaseModel):
    """Событие в очереди payments.new (payload outbox-записи)."""

    event_id: uuid.UUID
    payment_id: uuid.UUID
    occurred_at: datetime


class WebhookPayload(_FromPayment):
    """Тело уведомления, которое получает клиент."""

    event: str = "payment.processed"
    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    failure_reason: str | None
    processed_at: datetime | None
