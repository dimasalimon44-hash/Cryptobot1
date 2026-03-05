#!/bin/bash

echo "=== Установка MEXC Fair Scanner Bot ==="

# Останавливаем старый процесс если есть
echo "Останавливаем старый процесс..."
pkill -f mexc_fair_scanner.py || true

# Копируем сервис файл
echo "Копируем сервис файл..."
cp /root/Cryptobot1/mexcbot.service /etc/systemd/system/mexcbot.service

# Перезагружаем systemd
echo "Перезагружаем systemd..."
systemctl daemon-reload

# Включаем автозапуск
echo "Включаем автозапуск..."
systemctl enable mexcbot

# Запускаем сервис
echo "Запускаем сервис..."
systemctl start mexcbot

# Проверяем статус
echo ""
echo "=== Статус сервиса ==="
systemctl status mexcbot

echo ""
echo "=== Готово! ==="
echo "Полезные команды:"
echo "  Статус:      systemctl status mexcbot"
echo "  Остановить:  systemctl stop mexcbot"
echo "  Запустить:   systemctl start mexcbot"
echo "  Перезапуск:  systemctl restart mexcbot"
echo "  Логи:        journalctl -u mexcbot -f"