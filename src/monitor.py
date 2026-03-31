"""
monitor.py v4.2 — Мониторинг цен с валидацией
==============================================

НОВОЕ: Защита от мусорных цен (8 589 000 ₽ для стульев).

  ПРОБЛЕМА: Парсер иногда находит случайное большое число в JSON
  которое не является ценой товара. Например, 8 589 000 ₽ для стульев.

  РЕШЕНИЕ — двойная валидация:

  1. АБСОЛЮТНЫЙ ЛИМИТ:
     Цена > 1 000 000 ₽ для товара дешевле 50 000 ₽ при добавлении
     → явно мусор, пропускаем.

  2. ОТНОСИТЕЛЬНЫЙ ЛИМИТ (основной):
     Если новая цена ОТЛИЧАЕТСЯ от базовой более чем в 10 раз
     → считаем мусором, не сохраняем, не алертим.

     Пример: стулья стоили 88 000 ₽ → пришло 8 589 000 ₽
     8 589 000 / 88 000 = 97.6x → это явно ошибка парсера.

  3. ПЕРВАЯ ЦЕНА: валидация через разумный диапазон по маркетплейсу.
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.dirname(__file__))

from database import (
    init_db, save_price, save_alert, get_stats,
    is_first_check, get_baseline_price,
    get_previous_different_price, count_successful_checks
)
from notifier.telegram import (
    send_new_product_alert, send_price_drop_alert, send_message
)
from parsers import wildberries, ozon, yandex_market
from scraping_client import check_account_status, print_session_stats


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'config', 'products.json'
)

# Максимальный коэффициент изменения цены за одну проверку
# Если цена изменилась более чем в 10x — это мусор парсера
MAX_PRICE_RATIO = 10.0

# Абсолютный максимум цены (защита от 8.5 млн для стульев)
# Для действительно дорогих товаров можно поднять в products.json
ABSOLUTE_MAX_PRICE = 2_000_000


def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_parser(marketplace: str):
    return {
        'wildberries':   wildberries,
        'ozon':          ozon,
        'yandex_market': yandex_market,
        'ym':            yandex_market,
    }.get(marketplace.lower())


def _fmt(price: Optional[float]) -> str:
    if price is None:
        return 'н/д'
    return f'{price:,.0f} ₽'.replace(',', '\u202f')


def _is_price_sane(new_price: float, baseline_price: Optional[float],
                   max_price: float = ABSOLUTE_MAX_PRICE) -> tuple:
    """
    Проверяет разумность цены.

    Returns:
        (True, '') — цена разумная
        (False, причина) — цена подозрительная
    """
    # Абсолютный максимум
    if new_price > max_price:
        return False, (
            f"Цена {_fmt(new_price)} превышает максимум {_fmt(max_price)} — "
            f"это мусор парсера"
        )

    # Относительная проверка (только если есть базовая цена)
    if baseline_price and baseline_price > 0:
        ratio = new_price / baseline_price
        if ratio > MAX_PRICE_RATIO:
            return False, (
                f"Цена выросла в {ratio:.1f}x от базовой "
                f"({_fmt(baseline_price)} → {_fmt(new_price)}) — "
                f"мусор парсера, игнорируем"
            )
        # Также проверяем если цена упала в 10x — тоже подозрительно
        if ratio < (1 / MAX_PRICE_RATIO):
            return False, (
                f"Цена упала в {1/ratio:.1f}x от базовой "
                f"({_fmt(baseline_price)} → {_fmt(new_price)}) — "
                f"мусор парсера, игнорируем"
            )

    return True, ''


def check_single_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Проверяет один товар с валидацией цены.
    """
    pid       = product['id']
    name      = product['name']
    url       = product['url']
    mp        = product['marketplace']
    threshold = float(product.get('alert_threshold', 10))
    # Индивидуальный max_price из конфига (опционально)
    max_price = float(product.get('max_price', ABSOLUTE_MAX_PRICE))

    print(f'\n  📦 {name}')
    print(f'     URL: {url[:65]}')
    print(f'     {mp} | Порог: {threshold}%')

    first_time = is_first_check(pid)
    if first_time:
        print('     📌 ПЕРВАЯ ПРОВЕРКА')
    else:
        baseline = get_baseline_price(pid)
        checks   = count_successful_checks(pid)
        if baseline:
            print(f'     📊 Проверок: {checks} | Базовая: {_fmt(baseline["price"])}')

    # ── Парсинг ───────────────────────────────────────────
    parser = get_parser(mp)
    if not parser:
        msg = f'Неизвестный маркетплейс: {mp}'
        save_price(pid, name, url, mp, None, False, msg)
        return {'success': False, 'is_new': False, 'alert_sent': False, 'error': msg}

    result   = parser.fetch_price(url)
    price    = result.get('price')
    in_stock = result.get('in_stock', False)
    error    = result.get('error')

    parsed_name = result.get('name', '')
    if parsed_name and not name:
        name = parsed_name

    print(f'     Цена:    {_fmt(price)}')
    print(f'     Наличие: {"✅" if in_stock else "❌"}')
    if error:
        print(f'     ⚠️  {error[:120]}')

    # ── Валидация цены ────────────────────────────────────
    if price is not None:
        baseline = None if first_time else get_baseline_price(pid)
        baseline_price = baseline['price'] if baseline else None

        sane, reason = _is_price_sane(price, baseline_price, max_price)
        if not sane:
            print(f'     🚫 МУСОРНАЯ ЦЕНА: {reason}')
            # Сохраняем запись об ошибке (не цену)
            save_price(pid, name, url, mp, None, False,
                       f'Отклонена мусорная цена {_fmt(price)}: {reason}')
            return {
                'success':    False,
                'is_new':     first_time,
                'alert_sent': False,
                'price':      None,
                'error':      reason,
            }

    # ── Сохраняем в БД ────────────────────────────────────
    save_price(pid, name, url, mp, price, in_stock, error)

    if price is None:
        return {'success': False, 'is_new': first_time,
                'alert_sent': False, 'error': error}

    # ── ПЕРВАЯ ПРОВЕРКА → уведомление «добавлен» ─────────
    if first_time:
        print(f'     🆕 Новый товар — отправляем уведомление...')
        sent = send_new_product_alert(
            product_name=name, marketplace=mp,
            price=price, url=url, threshold=threshold,
        )
        if sent:
            print('     ✅ Уведомление о новом товаре отправлено')
        save_alert(pid, old_price=price, new_price=price,
                   change_percent=0.0, alert_type='new_product')
        return {'success': True, 'is_new': True, 'alert_sent': True,
                'price': price, 'change_percent': 0.0}

    # ── ПОВТОРНАЯ ПРОВЕРКА → сравниваем с базовой ─────────
    baseline     = get_baseline_price(pid)
    base_price   = baseline['price'] if baseline else price
    pct_vs_base  = ((price - base_price) / base_price) * 100

    # Для лога — изменение vs предыдущий замер
    prev = get_previous_different_price(pid, price)
    if prev and prev.get('price'):
        pct_prev = ((price - prev['price']) / prev['price']) * 100
        print(f'     📊 Vs предыдущей: {pct_prev:+.1f}% '
              f'({_fmt(prev["price"])} → {_fmt(price)})')

    print(f'     📊 Vs базовой:    {pct_vs_base:+.1f}% '
          f'({_fmt(base_price)} → {_fmt(price)})')

    # Алерт если снижение от базовой >= threshold
    if pct_vs_base < 0 and abs(pct_vs_base) >= threshold:
        print(f'     🚨 АЛЕРТ! -{abs(pct_vs_base):.1f}% >= {threshold}%')
        old_for_alert = prev['price'] if prev else base_price
        save_alert(pid, old_price=old_for_alert, new_price=price,
                   change_percent=pct_vs_base, alert_type='drop')
        sent = send_price_drop_alert(
            product_name=name, marketplace=mp,
            old_price=old_for_alert, new_price=price,
            change_percent=pct_vs_base, url=url,
            threshold=threshold, baseline_price=base_price,
        )
        if sent:
            print('     ✅ Алерт отправлен')
        return {'success': True, 'is_new': False, 'alert_sent': True,
                'price': price, 'change_percent': pct_vs_base}

    return {'success': True, 'is_new': False, 'alert_sent': False,
            'price': price, 'change_percent': pct_vs_base}


def _send_summary(product_results: list, stats: dict):
    """Итоговый отчёт в Telegram."""
    mp_icons = {'wildberries': '🍇', 'ozon': '🔵', 'yandex_market': '🟡'}

    def _fmt2(p): return f'{p:,.0f} ₽'.replace(',', '\u202f')

    now   = datetime.now().strftime('%d.%m.%Y %H:%M')
    lines = [f'📊 <b>Мониторинг цен — итог</b>', f'🕐 {now}', '']

    for r in product_results:
        icon = mp_icons.get(r['marketplace'], '🏪')
        name = r['name'][:35] + ('…' if len(r['name']) > 35 else '')
        if r['ok'] and r['price']:
            pct = r.get('change_pct')
            pct_str = f' ({pct:+.1f}%)' if pct is not None and pct != 0.0 else ''
            status = ('🆕 ' if r['is_new'] else '') + _fmt2(r['price']) + pct_str
        else:
            err = (r.get('error') or 'ошибка')[:55]
            status = f'❓ н/д ({err})'
        lines += [f'{icon} <b>{name}</b>', f'     {status}']

    lines += [
        '',
        f'✅ {stats["ok"]}/{stats["checked"]} | ❌ {stats["errors"]} ошибок'
        + (f' | 🆕 {stats["new_products"]}' if stats['new_products'] else '')
        + (f' | 🚨 {stats["alerts"]} алертов' if stats['alerts'] else ''),
    ]
    send_message('\n'.join(lines))


def run_monitoring():
    print('\n' + '═' * 65)
    print('🚀 МОНИТОРИНГ ЦЕН v4.2')
    print(f'⏰ {datetime.utcnow().strftime("%d.%m.%Y %H:%M:%S")} UTC')
    print('═' * 65)

    init_db()

    # ScraperAPI статус
    print('\n📡 ScraperAPI...')
    api = check_account_status()
    if not api['ok']:
        print(f'   ❌ {api["error"]}')
    else:
        left = api.get('requests_left', 0)
        print(f'   ✅ Осталось: {left} из {api.get("requests_limit", 0)}')
        if left < 100:
            send_message(f'⚠️ ScraperAPI мало запросов: {left}!')

    config   = load_config()
    products = [p for p in config['products'] if p.get('active', True)]
    print(f'\n📋 Товаров: {len(products)}')

    stats   = {'checked': 0, 'ok': 0, 'errors': 0, 'new_products': 0, 'alerts': 0}
    results = []

    for i, product in enumerate(products, 1):
        print(f'\n[{i}/{len(products)}]', end='')
        try:
            res = check_single_product(product)
            stats['checked'] += 1
            if res.get('success'):
                stats['ok'] += 1
            else:
                stats['errors'] += 1
            if res.get('is_new'):
                stats['new_products'] += 1
            if res.get('alert_sent') and not res.get('is_new'):
                stats['alerts'] += 1
            results.append({
                'name':        product.get('name', '?'),
                'marketplace': product.get('marketplace', ''),
                'price':       res.get('price'),
                'ok':          res.get('success', False),
                'error':       res.get('error', ''),
                'is_new':      res.get('is_new', False),
                'change_pct':  res.get('change_percent'),
            })
        except Exception as exc:
            stats['errors'] += 1
            print(f'\n     💥 {exc}')
            import traceback; traceback.print_exc()
            results.append({
                'name': product.get('name', '?'),
                'marketplace': product.get('marketplace', ''),
                'price': None, 'ok': False,
                'error': str(exc)[:80], 'is_new': False, 'change_pct': None,
            })

        if i < len(products):
            pause = 5 if product.get('marketplace') == 'wildberries' else 15
            print(f'     💤 {pause}с...')
            time.sleep(pause)

    # Дашборд
    print('\n📊 Дашборд...')
    try:
        from dashboard_generator import generate_dashboard
        generate_dashboard()
        print('   ✅ Готов')
    except Exception as e:
        print(f'   ⚠️  {e}')

    print_session_stats()
    _send_summary(results, stats)

    print('\n' + '═' * 65)
    print(f'✅ {stats["ok"]} / ❌ {stats["errors"]} / 🆕 {stats["new_products"]} / 🚨 {stats["alerts"]}')
    print('═' * 65 + '\n')
    return stats


if __name__ == '__main__':
    run_monitoring()
