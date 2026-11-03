"""Тестовый приёмник webhook'ов. Хранит полученное в памяти и умеет имитировать сбои.

POST /webhook            всегда 200
POST /webhook/flaky      первые FLAKY_FAILS (по умолчанию 2) запросов по каждому payment_id -> 503,
                         дальше 200. Демонстрирует retry с экспоненциальной задержкой.
POST /webhook/fail       всегда 500. Демонстрирует попадание в DLQ после 3 попыток.
GET  /received           всё, что дошло (с проверкой подписи)
DELETE /received         очистить
"""

import os
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from payment_service.config import get_settings
from payment_service.services.webhook import (
    ATTEMPT_HEADER,
    EVENT_ID_HEADER,
    SIGNATURE_HEADER,
    verify,
)

SECRET = get_settings().webhook_secret
FLAKY_FAILS = int(os.environ.get("FLAKY_FAILS", "2"))

app = FastAPI(title="webhook-sink")
received: list[dict[str, Any]] = []
flaky_counter: dict[str, int] = defaultdict(int)


async def _record(request: Request, route: str) -> dict[str, Any]:
    body = await request.body()
    return {
        "route": route,
        "received_at": datetime.now(UTC).isoformat(),
        "signature_valid": verify(body, SECRET, request.headers.get(SIGNATURE_HEADER)),
        "event_id": request.headers.get(EVENT_ID_HEADER),
        "attempt": request.headers.get(ATTEMPT_HEADER),
        "payload": await request.json() if body else None,
    }


@app.post("/webhook")
async def webhook(request: Request) -> dict[str, str]:
    received.append(await _record(request, "ok"))
    return {"status": "received"}


@app.post("/webhook/flaky")
async def webhook_flaky(request: Request) -> Response:
    entry = await _record(request, "flaky")
    payment_id = (entry["payload"] or {}).get("payment_id", "?")
    flaky_counter[payment_id] += 1
    if flaky_counter[payment_id] <= FLAKY_FAILS:
        return Response(status_code=503, content="try later")
    received.append(entry)
    return JSONResponse({"status": "received"})


@app.post("/webhook/fail")
async def webhook_fail() -> Response:
    return Response(status_code=500, content="always broken")


@app.get("/received")
async def list_received(payment_id: str | None = None) -> list[dict[str, Any]]:
    if payment_id is None:
        return received
    return [r for r in received if (r["payload"] or {}).get("payment_id") == payment_id]


@app.delete("/received", status_code=204)
async def clear() -> None:
    received.clear()
    flaky_counter.clear()
