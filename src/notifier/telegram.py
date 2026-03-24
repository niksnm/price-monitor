"""
notifier/telegram.py — отправка уведомлений через Telegram Bot API
"""

import os
import requests
from typing import Optional
from datetime import datetime


def send_message(text: str, parse_mode: str = "HTML") -> bool:
    """
    Отправляет сообщение в Telegram чат.

    Токен и chat_id берутся из переменных окружения:
      TELEGRAM_BOT_TOKEN — токен бота от @BotFather
      TELEGRAM_CHAT_ID   — ID вашего чата/канала

    Returns:
        True если сообщение отправлено успешно, False если ошибка
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    if not token or not chat_id:
        print("⚠️  TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы — "
              "уведомление пропущено")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": False,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        print(f"✅ Telegram уведомление отправлено")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"❌ Ошибка Telegram API: {e} — {resp.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка сети при отправке в Telegram: {e}")

    return False


def format_price_drop_alert(
    product_name: str,
    marketplace: str,
    old_price: float,
    new_price: float,
    change_percent: float,
    url: str,
    threshold: float
) -> str:
    """
    Форматирует красивое уведомление о падении цены.

    Пример вывода:
    🚨 СНИЖЕНИЕ ЦЕНЫ на 23.5%!

    📦 Nike Air Max 90
    🏪 Wildberries

    💰 Было:   8 500 ₽
    🔥 Стало:  6 500 ₽
    📉 Скидка: −2 000 ₽ (−23.5%)

    ⚡ Порог уведомления: 10%

    🔗 Перейти к товару
    🕐 22.03.2025 14:30
    """
    marketplace_icons = {
        "wildberries": "🍇 Wildberries",
        "ozon": "🔵 Ozon",
        "yandex_market": "🟡 Яндекс.Маркет",
    }
    mp_label = marketplace_icons.get(marketplace.lower(), f"🏪 {marketplace}")

    diff = new_price - old_price  # Отрицательное число
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # Форматирование чисел с пробелами (8500 → 8 500)
    def fmt(n: float) -> str:
        return f"{n:,.0f}".replace(",", " ")

    text = (
        f"🚨 <b>СНИЖЕНИЕ ЦЕНЫ на {abs(change_percent):.1f}%!</b>\n\n"
        f"📦 <b>{product_name}</b>\n"
        f"🏪 {mp_label}\n\n"
        f"💰 Было:   <s>{fmt(old_price)} ₽</s>\n"
        f"🔥 Стало:  <b>{fmt(new_price)} ₽</b>\n"
        f"📉 Скидка: <b>−{fmt(abs(diff))} ₽ (−{abs(change_percent):.1f}%)</b>\n\n"
        f"⚡ Порог уведомления: {threshold}%\n\n"
        f"🔗 <a href=\"{url}\">Перейти к товару</a>\n"
        f"🕐 {now}"
    )
    return text


def format_error_alert(product_name: str, marketplace: str,
                        error: str, url: str) -> str:
    """Форматирует уведомление об ошибке парсинга (отправляется раз в сутки)."""
    return (
        f"⚠️ <b>Ошибка парсинга</b>\n\n"
        f"📦 {product_name}\n"
        f"🏪 {marketplace}\n"
        f"❗ {error}\n\n"
        f"🔗 <a href=\"{url}\">Ссылка на товар</a>"
    )


def format_daily_summary(products_data: list) -> str:
    """
    Форматирует ежедневный отчёт со статусом всех товаров.
    products_data — список словарей с данными о товарах.
    """
    lines = ["📊 <b>Ежедневный отчёт по ценам</b>\n"]
    lines.append(f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n")

    for p in products_data:
        price = p.get("price")
        name = p.get("name", "—")[:40]
        change = p.get("change_percent", 0)

        if price is None:
            icon = "❓"
            price_str = "н/д"
        elif change < -5:
            icon = "📉"
            price_str = f"{price:,.0f} ₽ ({change:+.1f}%)".replace(",", " ")
        elif change > 5:
            icon = "📈"
            price_str = f"{price:,.0f} ₽ ({change:+.1f}%)".replace(",", " ")
        else:
            icon = "➡️"
            price_str = f"{price:,.0f} ₽".replace(",", " ")

        lines.append(f"{icon} {name}\n    {price_str}")

    return "\n".join(lines)


def send_price_drop_alert(
    product_name: str,
    marketplace: str,
    old_price: float,
    new_price: float,
    change_percent: float,
    url: str,
    threshold: float
) -> bool:
    """Главная функция — отправляет уведомление о падении цены."""
    text = format_price_drop_alert(
        product_name, marketplace, old_price,
        new_price, change_percent, url, threshold
    )
    return send_message(text)


def test_connection() -> bool:
    """Тестирует подключение к Telegram — отправляет тестовое сообщение."""
    msg = (
        "✅ <b>Тест подключения</b>\n\n"
        "Мониторинг цен успешно настроен!\n"
        "Уведомления о снижении цен будут приходить сюда.\n\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    )
    return send_message(msg)


if __name__ == "__main__":
    print("Отправляем тестовое сообщение...")
    result = test_connection()
    print("Успешно!" if result else "Ошибка — проверьте токен и chat_id")
