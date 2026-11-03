# Асинхронный сервис процессинга платежей

Тестовое задание. Сервис принимает платёж, отвечает `202 Accepted`, обрабатывает его в фоне через эмуляцию платёжного шлюза и присылает результат на `webhook_url` клиента.

Стек: FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL 16, RabbitMQ (FastStream 0.7), Alembic, Docker Compose.

## Быстрый старт

Нужны только Docker и Docker Compose.

```bash
make up          # сборка и запуск: postgres, rabbitmq, migrate, api, consumer, webhook-sink
make demo        # создать платёж, повторить запрос, дождаться результата, показать webhook
make test        # unit-тесты (без инфраструктуры)
make test-e2e    # e2e против поднятого стека: идемпотентность, retry, DLQ (~30 c)
make lint        # ruff
make logs        # логи api и consumer
make down        # остановить и удалить тома
```

| Что | Где |
|---|---|
| API | http://localhost:8000, Swagger: http://localhost:8000/docs |
| RabbitMQ management | http://localhost:15672 (guest / guest) |
| PostgreSQL | localhost:5432, `payments` / `payments` |
| webhook-sink (тестовый приёмник уведомлений) | http://localhost:8080/received |

Ключ API по умолчанию `dev-secret-key`, все значения переопределяются переменными окружения (см. `.env.example`).

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key`. Без него или с неверным ключом: `401`.

| Метод | Путь | Ответ |
|---|---|---|
| `POST` | `/api/v1/payments` | `202` `{payment_id, status, created_at}` |
| `GET` | `/api/v1/payments/{payment_id}` | `200` полная карточка платежа, `404` если нет |
| `GET` | `/health` | `200` `{"status":"ok"}`, без ключа: нужен docker healthcheck |

### Создание платежа

Заголовок `Idempotency-Key` обязателен (1..128 символов). Тело: `amount` (decimal > 0, 2 знака), `currency` (`RUB`/`USD`/`EUR`), `description`, `metadata` (любой JSON-объект), `webhook_url` (http/https).

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-secret-key" \
  -H "Idempotency-Key: order-42-attempt-1" \
  -H "Content-Type: application/json" \
  -d '{"amount":"1500.00","currency":"RUB","description":"Order #42",
       "metadata":{"order_id":42},"webhook_url":"http://webhook-sink:8080/webhook"}'
```

```
HTTP/1.1 202 Accepted
{"payment_id":"03454929-dfbf-4fc0-bcb3-ef540d3cf45b","status":"pending","created_at":"2026-11-02T11:31:07.012875Z"}
```

Поведение `Idempotency-Key`:

| Ситуация | Результат |
|---|---|
| Новый ключ | платёж создан, `202` |
| Тот же ключ, то же тело | тот же `payment_id`, `202`, заголовок `Idempotent-Replayed: true`, ничего не создаётся |
| Тот же ключ, другое тело | `422`, платёж не создаётся |
| Два одновременных запроса с одним ключом | один создаёт, второй получает его же (уникальный индекс + перечитывание) |

Тело сравнивается по sha256 канонического JSON: порядок ключей в `metadata` и запись `100.5` / `100.50` не считаются разницей.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/03454929-dfbf-4fc0-bcb3-ef540d3cf45b -H "X-API-Key: dev-secret-key"
```

```json
{
  "id": "03454929-dfbf-4fc0-bcb3-ef540d3cf45b",
  "amount": "1500.00", "currency": "RUB", "description": "Order #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42-attempt-1",
  "webhook_url": "http://webhook-sink:8080/webhook",
  "failure_reason": null,
  "created_at": "2026-11-02T11:31:07.012875Z",
  "processed_at": "2026-11-02T11:31:10.609182Z",
  "webhook_delivered_at": "2026-11-02T11:31:10.637591Z",
  "updated_at": "2026-11-02T11:31:10.638387Z"
}
```

### Webhook

`POST` на `webhook_url`, JSON. Успехом считается любой `2xx`.

```json
{
  "event": "payment.processed",
  "payment_id": "03454929-…", "status": "succeeded",
  "amount": "1500.00", "currency": "RUB", "description": "Order #42",
  "metadata": {"order_id": 42}, "failure_reason": null,
  "processed_at": "2026-11-02T11:31:10.609182Z"
}
```

| Заголовок | Назначение |
|---|---|
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(тело, WEBHOOK_SECRET)>`: клиент проверяет, что уведомление от нас |
| `X-Webhook-Event-Id` | id события; при повторной доставке тот же, клиент может дедуплицировать |
| `X-Webhook-Attempt` | номер попытки, 1..3 |

Доставка at-least-once: в редком случае (webhook доставлен, но процесс упал до записи `webhook_delivered_at`) клиент получит уведомление дважды с одним `X-Webhook-Event-Id`.

## Как это работает

```
клиент ──POST──> api ──[одна транзакция]──> payments (pending) + outbox
                                                  │
                      consumer-процесс:  outbox relay ──publish──> RabbitMQ: payments → payments.new
                                                                                   │
                                         consumer ◄────────────────────────────────┘
                                           1. шлюз (2-5 c, 90/10)  →  status, processed_at
                                           2. POST webhook         →  webhook_delivered_at
                                           ошибка → payments.retry.N (TTL) → снова payments.new
                                           3 неудачи → reject → payments.dlx → payments.dlq
```

Два процесса:

| Процесс | Команда | Что делает |
|---|---|---|
| `api` | `uvicorn payment_service.api.app:app` | HTTP, валидация, идемпотентность, запись платежа и outbox-события в одной транзакции. К RabbitMQ не подключается вообще |
| `consumer` | `python -m payment_service.worker` | Объявляет топологию RabbitMQ, запускает outbox relay (фоновая задача) и единственный обработчик очереди `payments.new` |

### Outbox pattern

`POST /payments` пишет строку в `payments` и строку в `outbox` в одной транзакции. Relay раз в 0,5 с выбирает неопубликованные события `FOR UPDATE SKIP LOCKED`, публикует их с publisher confirms (`persist=True`) и ставит `published_at`. Следствия:

- если RabbitMQ недоступен, API продолжает принимать платежи, события копятся в `outbox` и уходят после восстановления (проверено: `docker compose stop rabbitmq`, POST → 202, `start` → платёж обработан);
- relay можно запускать в нескольких экземплярах, `SKIP LOCKED` не даст им взять одну строку;
- при падении между confirm и commit событие опубликуется повторно, consumer это переживает (ниже).

Частичный индекс `ix_outbox_unpublished (created_at) WHERE published_at IS NULL` держит выборку быстрой при росте таблицы.

### Топология RabbitMQ

| Объект | Тип | Назначение |
|---|---|---|
| `payments` | direct exchange | основной; routing key `payments.new` |
| `payments.new` | queue | рабочая очередь consumer'а; `x-dead-letter-exchange = payments.dlx` |
| `payments.retry` | direct exchange | вход в retry-очереди; routing key `retry.N` |
| `payments.retry.2`, `payments.retry.3` | queue | TTL 2 с и 4 с, dead-letter обратно в `payments` / `payments.new` |
| `payments.dlx` | direct exchange | dead letter exchange |
| `payments.dlq` | queue | окончательно упавшие сообщения, никто не читает |

Очереди durable, сообщения persistent. Prefetch 10: обработка одного платежа это в основном ожидание (шлюз 2–5 с, webhook), поэтому consumer держит несколько сообщений параллельно; соединение с БД на время ожидания не удерживается.

### Retry и DLQ

Один механизм на уровне сообщений, без `sleep` внутри обработчика (consumer не блокируется, ожидание видно в RabbitMQ):

| Попытка | Когда | Если упала |
|---|---|---|
| 1 | сразу | копия сообщения с `x-attempt: 2` → `payments.retry.2`, оригинал ack |
| 2 | через 2 с | копия с `x-attempt: 3` → `payments.retry.3` |
| 3 | ещё через 4 с | `reject(requeue=False)` → RabbitMQ по DLX кладёт в `payments.dlq` |

`MAX_ATTEMPTS` и `RETRY_BASE_DELAY` настраиваются; retry-очереди создаются под нужное число попыток. Аргументы очереди (`x-message-ttl`) фиксируются при первом объявлении: если поменять `RETRY_BASE_DELAY` на уже работающем брокере, RabbitMQ ответит `PRECONDITION_FAILED` и consumer не стартует. Нужно удалить очереди `payments.retry.*` (или `make down` с томами) и запустить заново. В DLQ сообщение лежит с заголовками `x-attempt`, `x-last-error` и стандартным `x-death` (видно в management UI).

Что уходит в retry, а что нет:

| Событие | Действие |
|---|---|
| webhook ответил не-2xx, таймаут, сеть | retry |
| ошибка БД | retry |
| шлюз ответил «отказ» (10%) | **не ошибка**: платёж `failed` с `failure_reason`, webhook отправлен, сообщение ack |
| битый payload, платёж не найден | сразу в DLQ без повторов (`reject`) |

### Идемпотентность consumer'а

Сообщение может прийти повторно (retry, повторная публикация relay, redelivery после обрыва соединения). Обработчик проверяет каждый шаг:

1. платёж уже `succeeded`/`failed` → шлюз не вызывается повторно; запись результата это условный `UPDATE ... WHERE status = 'pending'`, так что при параллельной обработке одного события (redelivery после обрыва соединения) результат записывает только первый обработчик;
2. `webhook_delivered_at` уже стоит → webhook не шлётся повторно.

Поэтому при сбое webhook'а повтор отправляет только webhook, деньги второй раз не списываются (e2e-тест `test_flaky_webhook_is_retried_with_backoff` проверяет, что `status` и `processed_at` между попытками не менялись).

## Структура

```
src/payment_service/
  api/            FastAPI: app, auth (X-API-Key), routes/payments
  broker/         topology (exchanges, queues, retry/DLQ), consumer (обработчик)
  db/             SQLAlchemy models (payments, outbox), session
  domain/         enums, pydantic-схемы запросов/ответов/событий
  services/       payments (создание, идемпотентность), outbox (relay),
                  gateway (эмуляция шлюза), webhook (отправка, HMAC)
  worker.py       процесс consumer: FastStream app + relay
  config.py       настройки из env
alembic/          миграция 0001: payments, outbox
webhook_sink/     тестовый приёмник webhook'ов (не часть сервиса)
tests/            test_unit (без инфраструктуры), test_e2e (против compose-стека)
scripts/demo.sh   сценарий из make demo
```

## Настройки

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `API_KEY` | `dev-secret-key` | ключ для `X-API-Key` |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@postgres:5432/payments` | |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | |
| `WEBHOOK_SECRET` | `dev-webhook-secret` | секрет HMAC-подписи |
| `GATEWAY_DELAY_MIN` / `GATEWAY_DELAY_MAX` | `2.0` / `5.0` | задержка эмуляции шлюза, с |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `CONSUMER_PREFETCH` | `10` | сколько сообщений consumer обрабатывает одновременно |
| `MAX_ATTEMPTS` | `3` | попыток обработки до DLQ |
| `RETRY_BASE_DELAY` | `2.0` | задержка перед 2-й попыткой; дальше ×2 |
| `WEBHOOK_TIMEOUT` | `5.0` | таймаут POST на webhook, с |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` | `0.5` / `100` | relay |

## Тесты

`make test`: unit, без инфраструктуры. Отпечаток идемпотентности, валидация тела, расписание retry, аргументы retry-очередей, эмуляция шлюза, HMAC-подпись.

`make test-e2e` (стек должен быть поднят): 401 без ключа, 422 без `Idempotency-Key`, полный цикл создание → обработка → webhook с валидной подписью, повтор и конфликт ключа, 404, retry с backoff на «моргающем» webhook (sink отвечает 503 на первые две попытки, доставка на третьей), попадание в DLQ при вечно падающем webhook (проверяется через management API RabbitMQ).

Тестовый приёмник `webhook-sink` принимает на `/webhook` (всегда 200), `/webhook/flaky` (503 первые 2 раза по каждому платежу), `/webhook/fail` (всегда 500); всё полученное отдаёт на `GET /received`.

## Решения, которые стоит проговорить

- **Relay живёт в процессе consumer, а не в API.** API тогда не зависит от брокера и принимает платежи во время его простоя. Для нескольких реплик consumer'а relay безопасен благодаря `SKIP LOCKED`; при желании выносится в отдельный процесс без изменений в коде relay.
- **10% «ошибок» шлюза трактую как отказ платежа, а не как сбой.** Иначе 10% платежей ходили бы по кругу retry и падали в DLQ без бизнес-смысла. Технические ошибки (webhook, БД) ретраятся.
- **Повтор с тем же ключом возвращает `202`, а не `200`.** Ответ на повтор совпадает с оригинальным (как у Stripe); о повторе говорит заголовок `Idempotent-Replayed`.
- **Retry через TTL-очереди, а не `sleep` в обработчике.** Обработчик не держит сообщение unacked минуты, задержки видны в брокере, consumer можно масштабировать.
- **В DLQ через нативный reject, а не ручную публикацию.** Так в сообщении есть `x-death` с историей, и туда же попадают сообщения, упавшие по любой другой причине.
- **`/health` без ключа.** Это не бизнес-эндпоинт; он нужен docker healthcheck'у. Если «для всех эндпоинтов» понимать буквально, закрывается одной строкой.

Что бы добавил в production и сознательно не делал здесь: защиту от SSRF для `webhook_url` (сейчас consumer отправит POST на любой http-адрес, включая внутренние хосты сети; в демо приёмник сам живёт во внутренней сети, поэтому запрет приватных диапазонов сломал бы `make demo`, в production нужен allowlist или фильтр loopback/private/link-local), сохранение ссылки на транзакцию шлюза до смены статуса (сейчас при падении между вызовом шлюза и commit шлюз будет вызван повторно), очистка/архивация опубликованных outbox-строк, инструмент повторной отправки из DLQ, метрики (Prometheus) и трассировка, ключи API на клиента вместо одного статического, quorum queues, rate limiting.
