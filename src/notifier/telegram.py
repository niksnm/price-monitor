"""
notifier/telegram.py — Telegram уведомления v4
===============================================

ТИПЫ УВЕДОМЛЕНИЙ:
  1. 🆕 Новый товар — при первой успешной проверке
  2. 🚨 Цена упала  — когда падение >= threshold%
  3. ⚠️  Системные  — баланс API и прочее
"""

import os
import requests
from datetime import datetime
from typing import Optional


def _send(text: str, parse_mode: str = 'HTML') -> bool:
    """Отправляет сообщение в Telegram."""
    token   = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = os.environ.get('TELEGRAM_CHAT_ID', '').strip()
    if not token or not chat_id:
        print('⚠️  TELEGRAM не настроен — пропускаем уведомление')
        return False
    try:
        resp = requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text,
                  'parse_mode': parse_mode, 'disable_web_page_preview': False},
            timeout=15,
        )
        resp.raise_for_status()
        print('✅ Telegram: отправлено')
        return True
    except Exception as e:
        print(f'❌ Telegram ошибка: {e}')
        return False


def _fmt(price: float) -> str:
    return f'{price:,.0f}'.replace(',', '\u202f')


def _now() -> str:
    return datetime.now().strftime('%d.%m.%Y %H:%M')


MP_ICONS = {
    'wildberries':   '🍇 Wildberries',
    'ozon':          '🔵 Ozon',
    'yandex_market': '🟡 Яндекс.Маркет',
}


def send_new_product_alert(product_name: str, marketplace: str,
                           price: float, url: str, threshold: float) -> bool:
    """
    Уведомление при первой успешной проверке нового товара.
    Сообщает стартовую (базовую) цену с которой будут считаться все изменения.
    """
    mp = MP_ICONS.get(marketplace.lower(), f'🏪 {marketplace}')
    text = (
        f'🆕 <b>Новый товар добавлен в мониторинг!</b>\n\n'
        f'📦 <b>{product_name}</b>\n'
        f'🏪 {mp}\n\n'
        f'💰 Стартовая цена: <b>{_fmt(price)} ₽</b>\n'
        f'📊 Все изменения будут считаться от этой цены\n'
        f'⚡ Уведомлю при снижении на <b>{threshold}%</b> и более\n\n'
        f'🔗 <a href="{url}">Открыть товар</a>\n'
        f'🕐 {_now()}'
    )
    return _send(text)


def send_price_drop_alert(product_name: str, marketplace: str,
                          old_price: float, new_price: float,
                          change_percent: float, url: str, threshold: float,
                          baseline_price: Optional[float] = None) -> bool:
    """
    Уведомление о падении цены.
    Если есть baseline_price — показывает также изменение от стартовой цены.
    """
    mp = MP_ICONS.get(marketplace.lower(), f'🏪 {marketplace}')
    diff = new_price - old_price

    text = (
        f'🚨 <b>СНИЖЕНИЕ ЦЕНЫ на {abs(change_percent):.1f}%!</b>\n\n'
        f'📦 <b>{product_name}</b>\n'
        f'🏪 {mp}\n\n'
        f'💰 Было:   <s>{_fmt(old_price)} ₽</s>\n'
        f'🔥 Стало:  <b>{_fmt(new_price)} ₽</b>\n'
        f'📉 Скидка: <b>−{_fmt(abs(diff))} ₽ (−{abs(change_percent):.1f}%)</b>\n'
    )

    if baseline_price and abs(baseline_price - old_price) > 1:
        base_diff = new_price - baseline_price
        base_pct  = (base_diff / baseline_price) * 100
        if base_diff < 0:
            text += (
                f'\n📊 От стартовой цены ({_fmt(baseline_price)} ₽):\n'
                f'   <b>−{_fmt(abs(base_diff))} ₽ (−{abs(base_pct):.1f}%)</b>\n'
            )

    text += (
        f'\n⚡ Порог уведомления: {threshold}%\n\n'
        f'🔗 <a href="{url}">Перейти к товару</a>\n'
        f'🕐 {_now()}'
    )
    return _send(text)


def send_message(text: str) -> bool:
    return _send(text)


def test_connection() -> bool:
    return _send(f'✅ <b>Тест подключения Price Monitor</b>\n🕐 {_now()}')
