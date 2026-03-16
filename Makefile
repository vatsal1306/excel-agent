.PHONY: help up down restart logs ps shell clean prune

up:
	docker compose up -d --build --remove-orphans

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

clean:
	docker compose down -v
