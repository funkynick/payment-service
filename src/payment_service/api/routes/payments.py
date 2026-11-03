import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.api.deps import require_api_key
from payment_service.db.models import Payment
from payment_service.db.session import get_session
from payment_service.domain.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from payment_service.services import payments as payments_service

router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)]
)

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Создать платёж",
    responses={
        401: {"description": "Нет или неверный X-API-Key"},
        422: {"description": "Ошибка валидации или Idempotency-Key с другим телом"},
    },
)
async def create_payment(
    data: PaymentCreate,
    session: SessionDep,
    response: Response,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=128,
            description=(
                "Уникальный ключ запроса. Повтор с тем же ключом и телом вернёт тот же платёж."
            ),
        ),
    ],
) -> PaymentAccepted:
    try:
        payment, created = await payments_service.create_payment(session, data, idempotency_key)
    except payments_service.IdempotencyConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc
    if not created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted.model_validate(payment)


@router.get(
    "/{payment_id}",
    summary="Получить платёж",
    responses={404: {"description": "Платёж не найден"}},
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentRead:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentRead.model_validate(payment)
