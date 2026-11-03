"""Отправка webhook-уведомления клиенту с HMAC-подписью тела."""

import hashlib
import hmac
import logging

import httpx

from payment_service.domain.schemas import WebhookPayload

log = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-Webhook-Signature"
EVENT_ID_HEADER = "X-Webhook-Event-Id"
ATTEMPT_HEADER = "X-Webhook-Attempt"


class WebhookDeliveryError(Exception):
    """Не-2xx ответ или сетевая ошибка. Приводит к retry сообщения."""


def sign(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify(body: bytes, secret: str, signature: str | None) -> bool:
    return signature is not None and hmac.compare_digest(sign(body, secret), signature)


class WebhookSender:
    """Один httpx-клиент на процесс: keep-alive между доставками."""

    def __init__(self, secret: str, timeout: float) -> None:
        self._secret = secret
        self._client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def send(self, url: str, payload: WebhookPayload, event_id: str, attempt: int) -> None:
        body = payload.model_dump_json().encode()
        headers = {
            "Content-Type": "application/json",
            SIGNATURE_HEADER: sign(body, self._secret),
            EVENT_ID_HEADER: event_id,
            ATTEMPT_HEADER: str(attempt),
        }
        try:
            response = await self._client.post(url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{url}: {exc.__class__.__name__}: {exc}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"{url}: HTTP {response.status_code}")
        log.info("webhook delivered url=%s payment=%s attempt=%s", url, payload.payment_id, attempt)
