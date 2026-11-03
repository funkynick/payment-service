"""Процесс consumer: FastStream-приложение + outbox relay в фоне.

Запуск: python -m payment_service.worker
"""

import asyncio
import logging

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from payment_service.broker.consumer import PaymentProcessor, register_consumer
from payment_service.broker.topology import (
    DLQ_QUEUE,
    DLX_EXCHANGE,
    RETRY_EXCHANGE,
    retry_queue,
)
from payment_service.config import Settings, get_settings
from payment_service.db.session import engine, session_factory
from payment_service.logging import setup_logging
from payment_service.services.gateway import PaymentGatewayEmulator
from payment_service.services.outbox import OutboxRelay
from payment_service.services.webhook import WebhookSender

log = logging.getLogger(__name__)


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Очередь payments.new и exchange payments объявляет сам subscriber.
    Здесь - DLX/DLQ и retry-очереди."""
    dlx = await broker.declare_exchange(DLX_EXCHANGE)
    dlq = await broker.declare_queue(DLQ_QUEUE)
    await dlq.bind(dlx, routing_key=DLQ_QUEUE.routing_key)

    retry_exchange = await broker.declare_exchange(RETRY_EXCHANGE)
    retry_queues = []
    for attempt in range(2, settings.max_attempts + 1):
        schema = retry_queue(attempt, settings.retry_delay(attempt))
        queue = await broker.declare_queue(schema)
        await queue.bind(retry_exchange, routing_key=schema.routing_key)
        retry_queues.append(schema.name)
    log.info("topology declared: dlq=%s, retry queues=%s", DLQ_QUEUE.name, retry_queues)


def build_app() -> FastStream:
    settings = get_settings()
    setup_logging(settings.log_level)

    broker = RabbitBroker(settings.rabbitmq_url)
    webhook = WebhookSender(settings.webhook_secret, settings.webhook_timeout)
    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=PaymentGatewayEmulator(
            settings.gateway_delay_min, settings.gateway_delay_max, settings.gateway_success_rate
        ),
        webhook=webhook,
    )
    register_consumer(broker, processor, settings)

    app = FastStream(broker)
    relay = OutboxRelay(
        session_factory, broker, settings.outbox_poll_interval, settings.outbox_batch_size
    )
    stop = asyncio.Event()
    relay_task: asyncio.Task[None] | None = None

    @app.on_startup
    async def _startup() -> None:
        nonlocal relay_task
        # до старта consumer'а: subscriber объявит payments.new с DLX, DLX/DLQ должны существовать
        await broker.connect()
        await declare_topology(broker, settings)
        relay_task = asyncio.create_task(relay.run(stop), name="outbox-relay")

    @app.on_shutdown
    async def _shutdown() -> None:
        stop.set()
        if relay_task is not None:
            try:
                await asyncio.wait_for(relay_task, timeout=10)
            except (TimeoutError, asyncio.CancelledError):
                relay_task.cancel()
        await webhook.aclose()
        await engine.dispose()

    return app


app = build_app()

if __name__ == "__main__":
    asyncio.run(app.run())
