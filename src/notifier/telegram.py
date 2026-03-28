"""
notifier/telegram.py  v5  — Telegram уведомления
=================================================

ТИПЫ УВЕДОМЛЕНИЙ:
  1. 🆕 Новый товар         — при первой успешной проверке
  2. 🚨 Снижение цены       — когда падение >= threshold%
  3. 📊 Итог запуска        — КАЖДЫЙ запуск (новое в v5, раньше не было!)
  4. ⚠️  Системные          — предупреждения (мало запросов ScraperAPI и т.п.)

ПОЧЕМУ НЕ ПРИХОДИЛИ УВЕДОМЛЕНИЯ (было в v4):
  Функция format_daily_summary существовала но НИКОГДА не вызывалась.
  Уведомления шли только при:
    - первом добавлении товара (send_new_product_alert)
    - срабатывании порога (send_price_drop_alert)
  Если цены не менялись → Telegram молчал.

  В v5: monitor.py вызывает send_run_summary() после каждого запуска.
  Пользователь видит что мониторинг работает даже без снижений цен.
"""

import os
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any


# ─────────────────────────────────────────────────────────────
# БАЗОВАЯ ОТПРАВКА
# ─────────────────────────────────────────────────────────────

def _send(text: str, parse_mode: str = 'HTML') -> bool:
    """Отправляет сообщение в Telegram. Возвращает True при успехе."""
    token   = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = os.environ.get('TELEGRAM_CHAT_ID', '').strip()

    if not token or not chat_id:
        print('⚠️  Telegram не настроен (TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID пустые)')
        return False

    try:
        resp = requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={
                'chat_id':                  chat_id,
                'text':                     text,
                'parse_mode':               parse_mode,
                'disable_web_page_preview': False,
            },
            timeout=15,
        )
        resp.raise_for_status()
        print('✅ Telegram: сообщение отправлено')
        return True
    except requests.exceptions.HTTPError as e:
        body = ''
        try:
            body = resp.text[:200]
        except Exception:
            pass
        print(f'❌ Telegram HTTP ошибка: {e} | {body}')
        print('   → Проверьте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в GitHub Secrets')
    except requests.exceptions.RequestException as e:
        print(f'❌ Telegram сеть: {e}')
    return False


# ─────────────────────────────────────────────────────────────
# ФОРМАТИРОВАНИЕ
# ─────────────────────────────────────────────────────────────

def _fmt(price: float) -> str:
    """1299000 → '1 299 000'."""
    return f'{price:,.0f}'.replace(',', '\u202f')


def _now() -> str:
    return datetime.now().strftime('%d.%m.%Y %H:%M')


MP_ICONS = {
    'wildberries':   '🍇 Wildberries',
    'ozon':          '🔵 Ozon',
    'yandex_market': '🟡 Яндекс.Маркет',
}


# ─────────────────────────────────────────────────────────────
# УВЕДОМЛЕНИЕ 1 — НОВЫЙ ТОВАР
# ─────────────────────────────────────────────────────────────

def send_new_product_alert(product_name: str, marketplace: str,
                            price: float, url: str,
                            threshold: float) -> bool:
    """Отправляет уведомление когда товар впервые добавлен в мониторинг."""
    mp = MP_ICONS.get(marketplace.lower(), f'🏪 {marketplace}')
    text = (
        f'🆕 <b>Новый товар добавлен!</b>\n\n'
        f'📦 <b>{product_name}</b>\n'
        f'🏪 {mp}\n\n'
        f'💰 Стартовая цена: <b>{_fmt(price)} ₽</b>\n'
        f'⚡ Уведомлю при снижении на <b>{threshold}%</b> и более\n\n'
        f'🔗 <a href="{url}">Открыть товар</a>\n'
        f'🕐 {_now()}'
    )
    return _send(text)


# ─────────────────────────────────────────────────────────────
# УВЕДОМЛЕНИЕ 2 — СНИЖЕНИЕ ЦЕНЫ
# ─────────────────────────────────────────────────────────────

def send_price_drop_alert(product_name: str, marketplace: str,
                           old_price: float, new_price: float,
                           change_percent: float, url: str,
                           threshold: float,
                           baseline_price: Optional[float] = None) -> bool:
    """Отправляет уведомление о падении цены."""
    mp   = MP_ICONS.get(marketplace.lower(), f'🏪 {marketplace}')
    diff = new_price - old_price  # отрицательное число

    text = (
        f'🚨 <b>СНИЖЕНИЕ ЦЕНЫ на {abs(change_percent):.1f}%!</b>\n\n'
        f'📦 <b>{product_name}</b>\n'
        f'🏪 {mp}\n\n'
        f'💰 Было:   <s>{_fmt(old_price)} ₽</s>\n'
        f'🔥 Стало:  <b>{_fmt(new_price)} ₽</b>\n'
        f'📉 Скидка: <b>−{_fmt(abs(diff))} ₽ (−{abs(change_percent):.1f}%)</b>\n'
    )

    # Показываем изменение от стартовой цены если она отличается от предыдущей
    if baseline_price and abs(baseline_price - old_price) > 1:
        base_diff = new_price - baseline_price
        base_pct  = (base_diff / baseline_price) * 100
        if base_diff < 0:
            text += (
                f'\n📊 От стартовой цены ({_fmt(baseline_price)} ₽):\n'
                f'   <b>−{_fmt(abs(base_diff))} ₽ (−{abs(base_pct):.1f}%)</b>\n'
            )

    text += (
        f'\n⚡ Порог: {threshold}%\n\n'
        f'🔗 <a href="{url}">Перейти к товару</a>\n'
        f'🕐 {_now()}'
    )
    return _send(text)


# ─────────────────────────────────────────────────────────────
# УВЕДОМЛЕНИЕ 3 — ИТОГ ЗАПУСКА (новое в v5)
# ─────────────────────────────────────────────────────────────

def send_run_summary(products_results: List[Dict[str, Any]],
                      stats: Dict[str, int]) -> bool:
    """
    Отправляет краткий итог каждого запуска мониторинга.

    ПОЧЕМУ ВАЖНО:
      Без этого уведомления пользователь не знает работает ли
      мониторинг вообще, если цены не меняются.
      Теперь каждые 3 часа в Telegram приходит статус всех товаров.

    products_results — список словарей:
      {"name": str, "marketplace": str, "price": float|None,
       "change_percent": float|None, "error": str|None, "url": str}

    stats — {"checked": int, "ok": int, "errors": int, "alerts": int, "new": int}
    """
    lines = [f'📊 <b>Мониторинг цен — итог</b>\n🕐 {_now()}\n']

    for p in products_results:
        price   = p.get('price')
        name    = (p.get('name') or '—')[:35]
        mp      = p.get('marketplace', '')
        icon    = MP_ICONS.get(mp.lower(), '🏪').split()[0]  # только эмодзи
        change  = p.get('change_percent')
        error   = p.get('error')

        if price is None:
            status = '❓ н/д'
            if error:
                # Краткое описание ошибки (первые 40 символов)
                status += f' ({error[:40]})'
        else:
            price_str = f'{_fmt(price)} ₽'
            if change is not None and abs(change) >= 0.5:
                arrow = '📉' if change < 0 else '📈'
                price_str += f'  {arrow} {change:+.1f}%'
            status = price_str

        lines.append(f'{icon} <b>{name}</b>\n    {status}')

    # Итоговая строка
    summary_parts = [f'✅ {stats["ok"]}/{stats["checked"]}']
    if stats.get('errors', 0):
        summary_parts.append(f'❌ {stats["errors"]} ошибок')
    if stats.get('alerts', 0):
        summary_parts.append(f'🚨 {stats["alerts"]} алертов')
    if stats.get('new', 0):
        summary_parts.append(f'🆕 {stats["new"]} новых')

    lines.append('\n' + ' | '.join(summary_parts))

    return _send('\n'.join(lines))


# ─────────────────────────────────────────────────────────────
# УВЕДОМЛЕНИЕ 4 — СИСТЕМНОЕ
# ─────────────────────────────────────────────────────────────

def send_message(text: str) -> bool:
    """Отправляет произвольное системное сообщение."""
    return _send(text)


# ─────────────────────────────────────────────────────────────
# ТЕСТ ПОДКЛЮЧЕНИЯ
# ─────────────────────────────────────────────────────────────

def test_connection() -> bool:
    """Тест — проверяет что токен и chat_id работают."""
    return _send(
        f'✅ <b>Price Monitor — тест подключения</b>\n'
        f'Telegram настроен корректно!\n'
        f'🕐 {_now()}'
    )


if __name__ == '__main__':
    print('Отправляем тестовое сообщение...')
    ok = test_connection()
    print('Успешно!' if ok else 'Ошибка — проверьте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID')
