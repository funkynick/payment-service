"""Unit-тесты без инфраструктуры: идемпотентность, retry-расписание, шлюз, подпись webhook."""

import random
import uuid
from decimal import Decimal

import pytest

from payment_service.broker.topology import retry_queue, retry_routing_key
from payment_service.config import Settings
from payment_service.domain.schemas import PaymentCreate
from payment_service.services.gateway import PaymentGatewayEmulator
from payment_service.services.payments import request_fingerprint
from payment_service.services.webhook import sign, verify


def _payment(**overrides) -> PaymentCreate:
    base = {
        "amount": "100.50",
        "currency": "USD",
        "description": "test",
        "metadata": {"b": 2, "a": 1},
        "webhook_url": "http://example.com/hook",
    }
    return PaymentCreate(**{**base, **overrides})


def test_fingerprint_ignores_key_order_and_number_format():
    a = request_fingerprint(_payment(metadata={"a": 1, "b": 2}, amount="100.5"))
    b = request_fingerprint(_payment(metadata={"b": 2, "a": 1}, amount="100.50"))
    assert a == b


def test_fingerprint_changes_with_body():
    assert request_fingerprint(_payment()) != request_fingerprint(_payment(amount="100.51"))


def test_payment_create_rejects_bad_input():
    with pytest.raises(ValueError):
        _payment(amount="-1")
    with pytest.raises(ValueError):
        _payment(currency="GBP")
    with pytest.raises(ValueError):
        _payment(webhook_url="not a url")
    with pytest.raises(ValueError):
        _payment(extra_field=1)


def test_retry_schedule_is_exponential():
    s = Settings(max_attempts=3, retry_base_delay=2.0)
    assert [s.retry_delay(a) for a in (1, 2, 3, 4)] == [0.0, 2.0, 4.0, 8.0]


def test_retry_queue_ttl_and_dlx_back_to_main_exchange():
    q = retry_queue(2, 2.0)
    assert q.name == "payments.retry.2"
    assert q.routing_key == retry_routing_key(2) == "retry.2"
    assert q.arguments["x-message-ttl"] == 2000
    assert q.arguments["x-dead-letter-exchange"] == "payments"
    assert q.arguments["x-dead-letter-routing-key"] == "payments.new"


async def test_gateway_success_and_decline_are_results_not_exceptions():
    ok = PaymentGatewayEmulator(0, 0, success_rate=1.0, rng=random.Random(1))
    bad = PaymentGatewayEmulator(0, 0, success_rate=0.0, rng=random.Random(1))
    pid = uuid.uuid4()
    assert (await ok.charge(pid, Decimal("1"), "RUB")).success is True
    declined = await bad.charge(pid, Decimal("1"), "RUB")
    assert not declined.success
    assert declined.reason == "declined_by_gateway"


async def test_gateway_rate_is_roughly_honoured():
    gw = PaymentGatewayEmulator(0, 0, success_rate=0.9, rng=random.Random(42))
    results = [(await gw.charge(uuid.uuid4(), Decimal("1"), "RUB")).success for _ in range(500)]
    assert 0.85 < sum(results) / len(results) < 0.95


def test_webhook_signature_roundtrip():
    body = b'{"payment_id":"x"}'
    sig = sign(body, "s3cret")
    assert sig.startswith("sha256=")
    assert verify(body, "s3cret", sig)
    assert not verify(body, "other", sig)
    assert not verify(body + b" ", "s3cret", sig)
    assert not verify(body, "s3cret", None)
