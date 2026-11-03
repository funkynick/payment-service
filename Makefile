.PHONY: up down logs ps test test-e2e lint demo

up:            ## собрать и поднять всё
	docker compose up --build -d

down:          ## остановить и удалить тома
	docker compose down -v

logs:
	docker compose logs -f api consumer

ps:
	docker compose ps

test:          ## unit-тесты (без инфраструктуры)
	docker compose run --rm --no-deps api pytest -m "not e2e" -q

test-e2e:      ## e2e против поднятого стека (make up)
	docker compose run --rm --no-deps -e API_URL=http://api:8000 -e SINK_URL=http://webhook-sink:8080 -e RABBITMQ_API=http://rabbitmq:15672 api pytest -m e2e -q

lint:
	docker compose run --rm --no-deps api ruff check src tests webhook_sink

demo:          ## создать платёж и показать результат
	./scripts/demo.sh
