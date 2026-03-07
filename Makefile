# Makefile for FleaMarket‑AI

.PHONY: all build up down install-service start-service stop-service clean

all: build

build:
	docker build -t fleamarket-ai .

up:
	docker compose up -d

down:
	docker compose down

# Install systemd user service (host mode)
install-service:
	@mkdir -p ~/.config/systemd/user
	@cp fleamarket.service ~/.config/systemd/user/fleamarket.service
	@systemctl --user daemon-reload
	@systemctl --user enable fleamarket.service
	@echo "Service installed. Run 'make start-service' to start."

start-service:
	@systemctl --user start fleamarket.service

stop-service:
	@systemctl --user stop fleamarket.service

clean:
	docker compose down --rmi all --volumes --remove-orphans
	rm -rf __pycache__ logs/*.log db/*.db
