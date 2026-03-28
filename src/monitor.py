"""
monitor.py  v5  — Главный скрипт мониторинга цен
=================================================

ИСПРАВЛЕНИЯ v5:
  1. Добавлен send_run_summary() — теперь Telegram получает итог КАЖДОГО
     запуска, даже если цены не менялись. Раньше молчание = непонятно
     работает ли вообще.

  2. Результаты всех товаров собираются в products_results[]
     и передаются в send_run_summary().

  3. Импорт send_run_summary из notifier.telegram.

ЛОГИКА ПЕРВОЙ ЦЕНЫ (из v4, без изменений):
  Первая проверка → уведомление «Новый товар, стартовая цена X ₽».
  Повторные → сравнение с baseline (первой) ценой → алерт при падении.
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional, List

sys.path.insert(0, os.path.dirname(__file__))

from database import (
    init_db, save_price, save_alert,
    is_first_check, get_baseline_price,
    get_previous_different_price, count_successful_checks,
)
from notifier.telegram import (
    send_new_product_alert, send_price_drop_alert,
    send_run_summary, send_message,
)
from parsers import wildberries, ozon, yandex_market
from scraping_client import check_account_status, print_session_stats


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'config', 'products.json'
)


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


# ─────────────────────────────────────────────────────────────
# ОБРАБОТКА ОДНОГО ТОВАРА
# ─────────────────────────────────────────────────────────────

def check_single_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Полный цикл проверки одного товара.
    Возвращает словарь с результатами для статистики и итогового отчёта.
    """
    pid       = product['id']
    name      = product.get('name', '')
    url       = product['url']
    mp        = product['marketplace']
    threshold = float(product.get('alert_threshold', 10))

    print(f'\n  📦 {name}')
    print(f'     URL: {url[:70]}')
    print(f'     Маркетплейс: {mp} | Порог: {threshold}%')

    # Первая или повторная проверка?
    first_time = is_first_check(pid)
    if first_time:
        print('     📌 ПЕРВАЯ ПРОВЕРКА — новый товар')
    else:
        checks   = count_successful_checks(pid)
        baseline = get_baseline_price(pid)
        if baseline:
            print(f'     📊 Проверок: {checks} | '
                  f'Стартовая цена: {_fmt(baseline["price"])}')

    # Парсинг
    parser = get_parser(mp)
    if not parser:
        msg = f'Неизвестный маркетплейс: {mp}'
        print(f'     ❌ {msg}')
        save_price(pid, name, url, mp, None, False, msg)
        return {
            'success': False, 'is_new': False, 'alert_sent': False,
            'error': msg, 'name': name, 'price': None,
            'change_percent': None, 'url': url, 'marketplace': mp,
        }

    result   = parser.fetch_price(url)
    price    = result.get('price')
    in_stock = result.get('in_stock', False)
    error    = result.get('error')

    # Обновляем имя если парсер нашёл лучшее
    parsed_name = result.get('name', '')
    if parsed_name and not name:
        name = parsed_name

    print(f'     Цена:    {_fmt(price)}')
    print(f'     Наличие: {"✅ Есть" if in_stock else "❌ Нет"}')
    if error:
        print(f'     ⚠️  {error[:120]}')

    # Сохраняем в БД
    save_price(pid, name, url, mp, price, in_stock, error)

    if price is None:
        return {
            'success': False, 'is_new': first_time, 'alert_sent': False,
            'error': error, 'name': name, 'price': None,
            'change_percent': None, 'url': url, 'marketplace': mp,
        }

    # ── ПЕРВАЯ ПРОВЕРКА: уведомление «добавлен» ──────────────
    if first_time:
        print('     🆕 Отправляем уведомление о новом товаре...')
        sent = send_new_product_alert(
            product_name=name, marketplace=mp,
            price=price, url=url, threshold=threshold,
        )
        if sent:
            print('     ✅ Уведомление о новом товаре отправлено!')
        save_alert(pid, old_price=price, new_price=price,
                   change_percent=0.0, alert_type='new_product')
        return {
            'success': True, 'is_new': True, 'alert_sent': True,
            'price': price, 'change_percent': 0.0,
            'error': None, 'name': name, 'url': url, 'marketplace': mp,
        }

    # ── ПОВТОРНАЯ ПРОВЕРКА: сравниваем с базовой ─────────────
    baseline = get_baseline_price(pid)
    if not baseline or baseline.get('price') is None:
        print('     ⚠️  Базовая цена не найдена в БД')
        return {
            'success': True, 'is_new': False, 'alert_sent': False,
            'price': price, 'change_percent': None,
            'error': None, 'name': name, 'url': url, 'marketplace': mp,
        }

    base_price  = baseline['price']
    pct_vs_base = ((price - base_price) / base_price) * 100

    # Для лога — изменение vs предыдущий замер
    prev = get_previous_different_price(pid, price)
    if prev and prev.get('price'):
        pct_prev = ((price - prev['price']) / prev['price']) * 100
        print(f'     📊 Vs предыдущей:  {pct_prev:+.1f}% '
              f'({_fmt(prev["price"])} → {_fmt(price)})')

    print(f'     📊 Vs стартовой:   {pct_vs_base:+.1f}% '
          f'({_fmt(base_price)} → {_fmt(price)})')

    # Алерт если снижение от базовой >= threshold
    alert_sent = False
    if pct_vs_base < 0 and abs(pct_vs_base) >= threshold:
        print(f'     🚨 ПОРОГ! {abs(pct_vs_base):.1f}% >= {threshold}% — алерт...')

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
            print('     ✅ Алерт отправлен!')
        alert_sent = True

    elif pct_vs_base > 0:
        print('     📈 Цена выросла относительно стартовой')
    else:
        print('     ➡️  Цена без значительных изменений')

    return {
        'success': True, 'is_new': False, 'alert_sent': alert_sent,
        'price': price, 'change_percent': pct_vs_base,
        'error': error, 'name': name, 'url': url, 'marketplace': mp,
    }


# ─────────────────────────────────────────────────────────────
# ТОЧКА ВХОДА
# ─────────────────────────────────────────────────────────────

def run_monitoring():
    print('\n' + '═' * 65)
    print('🚀 ЗАПУСК МОНИТОРИНГА ЦЕН v5')
    print(f'⏰ {datetime.utcnow().strftime("%d.%m.%Y %H:%M:%S")} UTC')
    print('═' * 65)

    init_db()

    # ── ScraperAPI ────────────────────────────────────────────
    print('\n📡 Проверка ScraperAPI...')
    api_status = check_account_status()
    if not api_status['ok']:
        print(f'   ❌ {api_status["error"]}')
        print('   ⚠️  Проверьте SCRAPER_API_KEY в GitHub Secrets')
    else:
        left  = api_status.get('requests_left', 0)
        limit = api_status.get('requests_limit', 0)
        print(f'   ✅ Активен | Использовано: {api_status.get("requests_used", 0)} | '
              f'Осталось: {left} из {limit}')
        if left < 100:
            send_message(f'⚠️ ScraperAPI: мало запросов! Осталось: {left} из {limit}')

    # ── Загрузка товаров ──────────────────────────────────────
    config   = load_config()
    products = [p for p in config['products'] if p.get('active', True)]
    print(f'\n📋 Товаров к проверке: {len(products)}')

    stats: Dict[str, int] = {
        'checked': 0, 'ok': 0, 'errors': 0,
        'alerts': 0, 'new': 0,
    }
    # Список результатов для итогового Telegram-отчёта
    products_results: List[Dict[str, Any]] = []

    # ── Цикл по товарам ───────────────────────────────────────
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
                stats['new'] += 1
            if res.get('alert_sent') and not res.get('is_new'):
                stats['alerts'] += 1

            # Сохраняем для итогового отчёта
            products_results.append({
                'name':           res.get('name', product.get('name', '?')),
                'marketplace':    product.get('marketplace', ''),
                'price':          res.get('price'),
                'change_percent': res.get('change_percent'),
                'error':          res.get('error'),
                'url':            product.get('url', ''),
            })

        except Exception as exc:
            stats['errors'] += 1
            print(f'\n     💥 Критическая ошибка: {exc}')
            import traceback
            traceback.print_exc()
            products_results.append({
                'name':           product.get('name', '?'),
                'marketplace':    product.get('marketplace', ''),
                'price':          None,
                'change_percent': None,
                'error':          str(exc),
                'url':            product.get('url', ''),
            })

        # Пауза между товарами
        if i < len(products):
            pause = 5 if product.get('marketplace') == 'wildberries' else 15
            print(f'     💤 Пауза {pause}с...')
            time.sleep(pause)

    # ── Дашборд ───────────────────────────────────────────────
    print('\n📊 Генерация дашборда...')
    try:
        from dashboard_generator import generate_dashboard
        generate_dashboard()
        print('   ✅ Дашборд обновлён')
    except Exception as e:
        print(f'   ⚠️  Ошибка дашборда: {e}')

    # ── Итоговый отчёт в Telegram ─────────────────────────────
    # ИСПРАВЛЕНИЕ v5: этого не было в v4 — теперь всегда шлём итог
    print('\n📬 Отправляем итоговый отчёт в Telegram...')
    try:
        sent = send_run_summary(products_results, stats)
        if sent:
            print('   ✅ Итоговый отчёт отправлен')
        else:
            print('   ⚠️  Итоговый отчёт не отправлен')
            print('   → Проверьте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в Secrets')
    except Exception as e:
        print(f'   ⚠️  Ошибка отправки отчёта: {e}')

    # ── Статистика ScraperAPI ─────────────────────────────────
    print_session_stats()

    # ── Консольный итог ───────────────────────────────────────
    print('\n' + '═' * 65)
    print('📈 ИТОГИ:')
    print(f'   ✅ Проверено:  {stats["checked"]}')
    print(f'   ✅ Успешно:    {stats["ok"]}')
    print(f'   ❌ Ошибок:     {stats["errors"]}')
    print(f'   🆕 Новых:      {stats["new"]}')
    print(f'   🚨 Алертов:    {stats["alerts"]}')
    print('═' * 65 + '\n')

    return stats


if __name__ == '__main__':
    run_monitoring()
