#!/bin/bash
set -e

echo "=== [$(date)] Deploying Cryptobot ==="

# Переходим в папку с ботом
cd "$(dirname "$0")"

# Подтягиваем последние изменения
echo "→ git pull..."
git pull origin main

# Пересобираем и перезапускаем ТОЛЬКО бота (не трогаем сайт)
echo "→ Rebuilding and restarting cryptobot container..."
docker compose build cryptobot
docker compose up -d --no-deps cryptobot

echo "=== Done! Bot is running ==="
echo ""
echo "Полезные команды:"
echo "  Статус:   docker compose ps"
echo "  Логи:     docker compose logs -f cryptobot"
echo "  Стоп бот: docker compose stop cryptobot"
echo "  Старт:    docker compose start cryptobot"
