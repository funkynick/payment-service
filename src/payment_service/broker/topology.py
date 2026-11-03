"""Топология RabbitMQ.

  payments (direct) --payments.new--> [payments.new] --> consumer
       ^                                   | reject / x-death
       |                                   v
       |                        payments.dlx (direct) --payments.dead--> [payments.dlq]
       |
       +--payments.new-- [payments.retry.1] (ttl=base)   <--retry.1-- payments.retry (direct)
       +--payments.new-- [payments.retry.2] (ttl=base*2) <--retry.2--      ^
                                                                        consumer при ошибке

Повтор: consumer публикует сообщение в retry-очередь нужной попытки (с x-attempt+1),
по истечении TTL RabbitMQ сам перекладывает его обратно в payments.new.
После max_attempts неудачных попыток consumer делает reject без requeue,
и RabbitMQ по x-dead-letter-exchange уводит сообщение в payments.dlq.
"""

from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"

NEW_ROUTING_KEY = "payments.new"
DEAD_ROUTING_KEY = "payments.dead"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)

NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE.name,
        "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
    },
)

DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key=DEAD_ROUTING_KEY)


def retry_routing_key(attempt: int) -> str:
    return f"retry.{attempt}"


def retry_queue(attempt: int, delay_seconds: float) -> RabbitQueue:
    """Очередь ожидания перед попыткой номер `attempt` (2..max_attempts)."""
    return RabbitQueue(
        f"payments.retry.{attempt}",
        durable=True,
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": int(delay_seconds * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )
