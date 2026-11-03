"""E2E против поднятого стека: `make up` затем `make test-e2e`
(или локально: pytest -m e2e с API_URL/SINK_URL/RABBITMQ_API по умолчанию на localhost).

Эти тесты ждут реальных задержек шлюза (2-5 с) и retry (2 + 4 с), поэтому небыстрые.
"""

import asyncio
import time
import uuid

import httpx
import pytest

from tests.conftest import API_URL, RABBITMQ_API

pytestmark = pytest.mark.e2e

SINK_INTERNAL = "http://webhook-sink:8080"  # адрес, по которому consumer видит sink внутри compose


def _body(webhook_path: str = "/webhook", **overrides) -> dict:
    body = {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "e2e",
        "metadata": {"order_id": 42},
        "webhook_url": f"{SINK_INTERNAL}{webhook_path}",
    }
    body.update(overrides)
    return body


async def _wait_for(predicate, timeout: float, interval: float = 0.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


async def _wait_delivered(sink: httpx.AsyncClient, payment_id: str, timeout: float) -> list[dict]:
    async def delivered():
        r = await sink.get("/received", params={"payment_id": payment_id})
        return r.json() or None

    return await _wait_for(delivered, timeout)


async def _wait_terminal(api: httpx.AsyncClient, payment_id: str, timeout: float = 30) -> dict:
    async def done():
        r = await api.get(f"/api/v1/payments/{payment_id}")
        r.raise_for_status()
        data = r.json()
        return data if data["status"] != "pending" else None

    return await _wait_for(done, timeout)


async def test_requires_api_key():
    async with httpx.AsyncClient(base_url=API_URL) as anon:
        r = await anon.post("/api/v1/payments", json=_body(), headers={"Idempotency-Key": "k"})
        assert r.status_code == 401
        r = await anon.get(f"/api/v1/payments/{uuid.uuid4()}")
        assert r.status_code == 401
        r = await anon.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
        assert r.status_code == 401


async def test_idempotency_key_is_required(api):
    r = await api.post("/api/v1/payments", json=_body())
    assert r.status_code == 422


async def test_create_process_and_webhook(api, sink):
    key = str(uuid.uuid4())
    r = await api.post("/api/v1/payments", json=_body(), headers={"Idempotency-Key": key})
    assert r.status_code == 202, r.text
    accepted = r.json()
    assert accepted["status"] == "pending"
    payment_id = accepted["payment_id"]

    # повтор с тем же ключом и телом: тот же платёж, ничего нового
    r2 = await api.post("/api/v1/payments", json=_body(), headers={"Idempotency-Key": key})
    assert r2.status_code == 202
    assert r2.json()["payment_id"] == payment_id
    assert r2.headers.get("Idempotent-Replayed") == "true"

    # тот же ключ, другое тело: конфликт
    r3 = await api.post(
        "/api/v1/payments", json=_body(amount="1.00"), headers={"Idempotency-Key": key}
    )
    assert r3.status_code == 422

    payment = await _wait_terminal(api, payment_id)
    assert payment["status"] in ("succeeded", "failed")
    assert payment["processed_at"] is not None
    assert payment["metadata"] == {"order_id": 42}
    assert payment["amount"] == "1500.00"

    hook = (await _wait_delivered(sink, payment_id, timeout=15))[0]
    assert hook["signature_valid"] is True
    assert hook["payload"]["status"] == payment["status"]
    assert hook["payload"]["payment_id"] == payment_id
    assert hook["attempt"] == "1"

    r = await api.get(f"/api/v1/payments/{payment_id}")
    assert r.json()["webhook_delivered_at"] is not None


async def test_get_unknown_payment(api):
    r = await api.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert r.status_code == 404


async def test_flaky_webhook_is_retried_with_backoff(api, sink):
    """sink отвечает 503 на первые 2 попытки -> доставка на 3-й (2 c + 4 c задержки)."""
    r = await api.post(
        "/api/v1/payments",
        json=_body("/webhook/flaky"),
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    payment_id = r.json()["payment_id"]
    payment = await _wait_terminal(api, payment_id)

    entries = await _wait_delivered(sink, payment_id, timeout=40)
    assert entries[0]["attempt"] == "3"
    # шлюз не вызывался повторно: статус и processed_at не менялись между попытками
    r = await api.get(f"/api/v1/payments/{payment_id}")
    assert r.json()["processed_at"] == payment["processed_at"]
    assert r.json()["status"] == payment["status"]


async def test_dead_webhook_ends_in_dlq(api):
    async with httpx.AsyncClient(base_url=RABBITMQ_API, auth=("guest", "guest"), timeout=10) as mq:

        async def dlq_count() -> int:
            r = await mq.get("/api/queues/%2F/payments.dlq")
            r.raise_for_status()
            return int(r.json().get("messages", 0))

        before = await dlq_count()
        r = await api.post(
            "/api/v1/payments",
            json=_body("/webhook/fail"),
            headers={"Idempotency-Key": str(uuid.uuid4())},
        )
        payment_id = r.json()["payment_id"]
        await _wait_terminal(api, payment_id)

        async def grew():
            # 3 попытки: сразу, +2 с, +4 с; плюс лаг статистики management API
            return (await dlq_count()) > before or None

        await _wait_for(grew, timeout=60, interval=2)
        r = await api.get(f"/api/v1/payments/{payment_id}")
        assert r.json()["webhook_delivered_at"] is None
