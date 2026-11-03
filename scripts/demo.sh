#!/usr/bin/env bash
# Создаёт платёж, повторяет запрос с тем же Idempotency-Key, ждёт обработки, показывает webhook.
set -euo pipefail
API=${API_URL:-http://localhost:8000}
SINK=${SINK_URL:-http://localhost:8080}
KEY=${API_KEY:-dev-secret-key}
IDEM=$(uuidgen | tr '[:upper:]' '[:lower:]')

echo "1) POST /api/v1/payments  (Idempotency-Key: $IDEM)"
RESP=$(curl -s -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" -H "Idempotency-Key: $IDEM" -H "Content-Type: application/json" \
  -d '{"amount":"1500.00","currency":"RUB","description":"Order #42","metadata":{"order_id":42},"webhook_url":"http://webhook-sink:8080/webhook"}')
echo "$RESP"
PID=$(echo "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["payment_id"])')

echo; echo "2) повтор с тем же ключом -> тот же payment_id, заголовок Idempotent-Replayed"
curl -s -i -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" -H "Idempotency-Key: $IDEM" -H "Content-Type: application/json" \
  -d '{"amount":"1500.00","currency":"RUB","description":"Order #42","metadata":{"order_id":42},"webhook_url":"http://webhook-sink:8080/webhook"}' | grep -iE "^HTTP|Idempotent-Replayed|payment_id"

echo; echo "3) ждём обработку (2-5 c)..."
for _ in $(seq 1 30); do
  STATUS=$(curl -s "$API/api/v1/payments/$PID" -H "X-API-Key: $KEY" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  [ "$STATUS" != "pending" ] && break
  sleep 1
done
echo "GET /api/v1/payments/$PID:"
curl -s "$API/api/v1/payments/$PID" -H "X-API-Key: $KEY" | python3 -m json.tool

echo; echo "4) что получил webhook-sink:"
curl -s "$SINK/received?payment_id=$PID" | python3 -m json.tool
