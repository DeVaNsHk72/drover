COMPOSE = docker compose -f deploy/docker-compose.yml
.PHONY : redis-up redis-down test lint

redis-up:
	$(COMPOSE) up -d redis
redis-down:
	$(COMPOSE) down
test:
	go test -race -count=1 ./...
lint:
	go vet ./...
	