"""
monitor.py v4 — Главный скрипт мониторинга цен
===============================================

НОВАЯ ЛОГИКА ПЕРВОЙ ЦЕНЫ:
  Проблема старой версии: при добавлении нового товара скрипт
  просто сохранял цену и молчал. Пользователь не знал что товар
  начал отслеживаться и какова стартовая цена.

  Новая логика:
  ┌─────────────────────────────────────────────────────────┐
  │ ПЕРВАЯ ПРОВЕРКА (товар ещё не был в БД):                │
  │   1. Получить цену                                      │
  │   2. Сохранить в БД как обычно                          │
  │   3. Отправить в Telegram: «Новый товар, цена: X ₽»     │
  │   4. ВСЕ будущие изменения считать от ЭТОЙ первой цены  │
  └─────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────┐
  │ ПОВТОРНЫЕ ПРОВЕРКИ (товар уже есть в БД):               │
  │   1. Получить цену                                      │
  │   2. Сохранить в БД                                     │
  │   3. Сравнить с ПЕРВОЙ (базовой) ценой:                 │
  │      - Если упала >= threshold% → алерт в Telegram      │
  │      - В алерте показать и разницу от первой цены       │
  └─────────────────────────────────────────────────────────┘

  Почему сравниваем с ПЕРВОЙ ценой, а не с предыдущим замером?
  - Предыдущий замер мог быть тем же числом (цена не менялась)
  - При сравнении с первой ценой видно ПОЛНЫЙ масштаб скидки
  - Пользователь понимает: «добавил за 8500, сейчас 6500 (-24%)»
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

    Шаги:
      1. Определить — первая проверка или повторная
      2. Получить цену через парсер
      3. Сохранить в БД
      4. Для первой проверки → уведомление «добавлен»
         Для повторной      → сравнить с базовой ценой, алерт если нужно

    Возвращает словарь с результатами для статистики.
    """
    pid       = product['id']
    name      = product['name']
    url       = product['url']
    mp        = product['marketplace']
    threshold = float(product.get('alert_threshold', 10))

    print(f'\n  📦 {name}')
    print(f'     URL: {url[:65]}')
    print(f'     Маркетплейс: {mp} | Порог: {threshold}%')

    # ── Шаг 1: Первая проверка или повторная? ────────────
    first_time = is_first_check(pid)
    if first_time:
        print('     📌 ПЕРВАЯ ПРОВЕРКА — новый товар')
    else:
        checks = count_successful_checks(pid)
        baseline = get_baseline_price(pid)
        if baseline:
            print(f'     📊 Проверок всего: {checks} | '
                  f'Стартовая цена: {_fmt(baseline["price"])}')

    # ── Шаг 2: Парсинг ───────────────────────────────────
    parser = get_parser(mp)
    if not parser:
        msg = f'Неизвестный маркетплейс: {mp}'
        print(f'     ❌ {msg}')
        save_price(pid, name, url, mp, None, False, msg)
        return {'success': False, 'is_new': False, 'alert_sent': False, 'error': msg}

    result   = parser.fetch_price(url)
    price    = result.get('price')
    in_stock = result.get('in_stock', False)
    error    = result.get('error')

    # Если парсер нашёл более точное название — обновляем
    parsed_name = result.get('name', '')
    if parsed_name and not name:
        name = parsed_name

    print(f'     Цена:    {_fmt(price)}')
    print(f'     Наличие: {"✅ Есть" if in_stock else "❌ Нет/Неизвестно"}')
    if error:
        print(f'     ⚠️  {error[:120]}')

    # ── Шаг 3: Сохраняем в БД ────────────────────────────
    save_price(pid, name, url, mp, price, in_stock, error)

    # Если цену не получили — дальше нечего делать
    if price is None:
        return {'success': False, 'is_new': first_time,
                'alert_sent': False, 'error': error}

    # ── Шаг 4a: ПЕРВАЯ ПРОВЕРКА — уведомление «добавлен» ─
    if first_time:
        print(f'     🆕 Отправляем уведомление о новом товаре...')
        sent = send_new_product_alert(
            product_name=name,
            marketplace=mp,
            price=price,
            url=url,
            threshold=threshold,
        )
        if sent:
            print('     ✅ Уведомление о новом товаре отправлено!')
        # Сохраняем в алерты как событие добавления
        save_alert(pid, old_price=price, new_price=price,
                   change_percent=0.0, alert_type='new_product')
        return {'success': True, 'is_new': True, 'alert_sent': True,
                'price': price, 'change_percent': 0.0}

    # ── Шаг 4b: ПОВТОРНАЯ ПРОВЕРКА — сравниваем с базовой ─
    baseline = get_baseline_price(pid)

    if not baseline or baseline.get('price') is None:
        # Нет базовой цены (теоретически не должно случиться)
        print('     ⚠️  Базовая цена не найдена в БД')
        return {'success': True, 'is_new': False, 'alert_sent': False, 'price': price}

    base_price   = baseline['price']
    pct_vs_base  = ((price - base_price) / base_price) * 100

    # Для лога — также показываем изменение vs предыдущий замер
    prev = get_previous_different_price(pid, price)
    if prev and prev.get('price'):
        pct_vs_prev = ((price - prev['price']) / prev['price']) * 100
        print(f'     📊 Vs предыдущей:  {pct_vs_prev:+.1f}% '
              f'({_fmt(prev["price"])} → {_fmt(price)})')

    print(f'     📊 Vs стартовой:   {pct_vs_base:+.1f}% '
          f'({_fmt(base_price)} → {_fmt(price)})')

    # Алерт если снижение от базовой цены >= threshold
    if pct_vs_base < 0 and abs(pct_vs_base) >= threshold:
        print(f'     🚨 ПОРОГ СРАБОТАЛ! '
              f'{abs(pct_vs_base):.1f}% >= {threshold}% — отправляем...')

        # Для красивого уведомления: «было» = предыдущая цена
        # «стало» = текущая цена, дополнительно — от стартовой
        old_for_alert = prev['price'] if prev else base_price

        save_alert(pid, old_price=old_for_alert, new_price=price,
                   change_percent=pct_vs_base, alert_type='drop')

        sent = send_price_drop_alert(
            product_name=name,
            marketplace=mp,
            old_price=old_for_alert,
            new_price=price,
            change_percent=pct_vs_base,
            url=url,
            threshold=threshold,
            baseline_price=base_price,
        )
        if sent:
            print('     ✅ Алерт о снижении цены отправлен!')

        return {'success': True, 'is_new': False, 'alert_sent': True,
                'price': price, 'change_percent': pct_vs_base}

    elif pct_vs_base > 0:
        print(f'     📈 Цена выросла относительно стартовой')
    else:
        print(f'     ➡️  Цена на уровне стартовой или незначительно изменилась')

    return {'success': True, 'is_new': False, 'alert_sent': False,
            'price': price, 'change_percent': pct_vs_base}


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def _send_summary(product_results: list, stats: dict):
    """
    Отправляет итоговый отчёт в Telegram после каждого запуска мониторинга.

    Формат сообщения:
      📊 Мониторинг цен — итог
      🕐 30.03.2026 07:34

      🍇 Джоггеры джинсовые
           45 990 ₽ (→ 0%)
      🔵 Профиль решетки Opel
           ❓ н/д (Ozon API не ответил)
      ...

      ✅ 3/5 | ❌ 2 ошибки
    """
    mp_icons = {
        'wildberries':   '🍇',
        'ozon':          '🔵',
        'yandex_market': '🟡',
    }

    def _fmt(p: float) -> str:
        return f'{p:,.0f} ₽'.replace(',', '\u202f')

    now = datetime.now().strftime('%d.%m.%Y %H:%M')
    lines = [
        f'📊 <b>Мониторинг цен — итог</b>',
        f'🕐 {now}',
        '',
    ]

    for r in product_results:
        icon = mp_icons.get(r['marketplace'], '🏪')
        name = r['name'][:35] + ('...' if len(r['name']) > 35 else '')

        if r['ok'] and r['price']:
            pct = r.get('change_pct')
            if pct is not None and pct != 0.0:
                pct_str = f' ({pct:+.1f}%)'
            else:
                pct_str = ''
            status = f'{_fmt(r["price"])}{pct_str}'
            if r['is_new']:
                status = f'🆕 {status}'
        else:
            # Обрезаем ошибку до разумной длины
            err = (r.get('error') or 'неизвестная ошибка')[:60]
            status = f'❓ н/д ({err})'

        lines.append(f'{icon} <b>{name}</b>')
        lines.append(f'     {status}')

    lines += [
        '',
        f'✅ {stats["ok"]}/{stats["checked"]} | '
        f'❌ {stats["errors"]} ошибок'
        + (f' | 🆕 {stats["new_products"]} новых' if stats['new_products'] else '')
        + (f' | 🚨 {stats["alerts"]} алертов' if stats['alerts'] else ''),
    ]

    send_message('\n'.join(lines))


def run_monitoring():
    print('\n' + '═' * 65)
    print('🚀 ЗАПУСК МОНИТОРИНГА ЦЕН v4')
    print(f'⏰ {datetime.utcnow().strftime("%d.%m.%Y %H:%M:%S")} UTC')
    print('═' * 65)

    init_db()

    # ── Проверяем ScraperAPI ──────────────────────────────
    print('\n📡 Проверка ScraperAPI...')
    api_status = check_account_status()
    if not api_status['ok']:
        print(f'   ❌ {api_status["error"]}')
        print('   ⚠️  Проверьте SCRAPER_API_KEY в GitHub Secrets')
    else:
        left  = api_status.get('requests_left', 0)
        limit = api_status.get('requests_limit', 0)
        print(f'   ✅ Активен | Использовано: {api_status.get("requests_used",0)} | '
              f'Осталось: {left} из {limit}')
        if left < 100:
            send_message(f'⚠️ ScraperAPI: осталось мало запросов: {left}!')

    # ── Загружаем товары ──────────────────────────────────
    config   = load_config()
    products = [p for p in config['products'] if p.get('active', True)]
    print(f'\n📋 Товаров к проверке: {len(products)}')

    stats = {'checked': 0, 'ok': 0, 'errors': 0,
             'new_products': 0, 'alerts': 0}

    # Детальные результаты для итогового Telegram-отчёта
    product_results = []  # [(имя, маркетплейс, цена, ок?, ошибка)]

    for i, product in enumerate(products, 1):
        print(f'\n[{i}/{len(products)}]', end='')
        try:
            res = check_single_product(product)
            stats['checked'] += 1

            ok    = res.get('success', False)
            price = res.get('price')
            err   = res.get('error', '')

            if ok:
                stats['ok'] += 1
            else:
                stats['errors'] += 1

            if res.get('is_new'):
                stats['new_products'] += 1

            if res.get('alert_sent') and not res.get('is_new'):
                stats['alerts'] += 1

            product_results.append({
                'name':        product.get('name', '?'),
                'marketplace': product.get('marketplace', ''),
                'price':       price,
                'ok':          ok,
                'error':       err,
                'is_new':      res.get('is_new', False),
                'change_pct':  res.get('change_percent'),
            })

        except Exception as exc:
            stats['errors'] += 1
            err_text = str(exc)
            print(f'\n     💥 Критическая ошибка: {err_text}')
            import traceback
            traceback.print_exc()
            product_results.append({
                'name':        product.get('name', '?'),
                'marketplace': product.get('marketplace', ''),
                'price':       None,
                'ok':          False,
                'error':       err_text[:120],
                'is_new':      False,
                'change_pct':  None,
            })

        # Пауза между товарами
        if i < len(products):
            pause = 5 if product.get('marketplace') == 'wildberries' else 15
            print(f'     💤 Пауза {pause}с...')
            time.sleep(pause)

    # ── Дашборд ───────────────────────────────────────────
    print('\n📊 Генерация дашборда...')
    try:
        from dashboard_generator import generate_dashboard
        generate_dashboard()
        print('   ✅ Дашборд обновлён')
    except Exception as e:
        print(f'   ⚠️  Ошибка дашборда: {e}')

    # ── Статистика ScraperAPI ─────────────────────────────
    print_session_stats()

    # ── Итоговый Telegram-отчёт ───────────────────────────
    _send_summary(product_results, stats)

    # ── Итоги в консоль ───────────────────────────────────
    print('\n' + '═' * 65)
    print('📈 ИТОГИ:')
    print(f'   ✅ Проверено:    {stats["checked"]}')
    print(f'   ✅ Успешно:      {stats["ok"]}')
    print(f'   ❌ Ошибок:       {stats["errors"]}')
    print(f'   🆕 Новых:        {stats["new_products"]}')
    print(f'   🚨 Алертов:      {stats["alerts"]}')
    print('═' * 65 + '\n')

    return stats


if __name__ == '__main__':
    run_monitoring()
