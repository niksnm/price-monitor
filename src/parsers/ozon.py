"""
parsers/ozon.py  v4  — Ozon через ScraperAPI (внутренний API + HTML fallback)
==============================================================================

ПОЧЕМУ ПРЕДЫДУЩИЕ ВЕРСИИ НЕ РАБОТАЛИ:
  Ozon — это React SPA. Вся торговая информация (цена, наличие)
  хранится в JSON-объекте который React читает при запуске.
  Старый код искал JSON-LD и CSS-атрибуты — Ozon их не генерирует.

СТРАТЕГИЯ v4 (два уровня):

  Уровень 1 — Внутренний API Ozon (быстрый, ~10 сек):
    Ozon имеет эндпоинт /api/composer-api.bx/page/json/v2?url=/product/...
    Он возвращает JSON со всеми данными страницы.
    НЕ требует JS-рендеринга → дешевле и быстрее.

  Уровень 2 — HTML с JS-рендерингом (медленный, ~60 сек):
    Если API не вернул цену — ScraperAPI запускает Chrome,
    выполняет JS, отдаёт готовый HTML.
    Ищем JSON внутри <script id="__NUXT_DATA__"> и похожих тегов.
    Дополнительно — regex по raw HTML для "finalPrice":45990.
"""

import re
import json
import sys
import os
from collections import Counter
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import scrape_url


# ─────────────────────────────────────────────────────────────
# КЛЮЧИ ЦЕН — в порядке приоритета
# ─────────────────────────────────────────────────────────────

# Эти ключи почти всегда означают финальную цену товара
HIGH_KEYS = {
    'finalPrice', 'cardPrice', 'sellPrice', 'salePrice',
    'discountedPrice', 'priceWithCard', 'offerPrice', 'sellingPrice'
}
# Эти ключи иногда означают цену, но могут быть и другим числом
MED_KEYS = {
    'price', 'currentPrice', 'minPrice', 'basePrice'
}


def _to_price(val) -> Optional[float]:
    """Преобразует значение в цену или возвращает None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 50 <= f <= 10_000_000 else None
    if isinstance(val, str):
        s = re.sub(r'[^\d.,]', '', val.replace('\xa0', '').replace('\u202f', ''))
        if not s:
            return None
        s = s.replace(',', '.')
        parts = s.split('.')
        if len(parts) > 2:
            s = ''.join(parts[:-1]) + '.' + parts[-1]
        try:
            f = float(s)
            return f if 50 <= f <= 10_000_000 else None
        except ValueError:
            return None
    return None


def _collect_prices(obj, depth: int = 0) -> List[Tuple[str, float, str]]:
    """
    Рекурсивно обходит JSON объект и собирает все найденные цены.
    Возвращает [(ключ, цена, приоритет), ...].
    """
    if depth > 20:
        return []
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in HIGH_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'high'))
            elif k in MED_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'med'))
            results.extend(_collect_prices(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj[:100]:
            results.extend(_collect_prices(item, depth + 1))
    return results


def _best_price(prices: List[Tuple[str, float, str]]) -> Optional[float]:
    """Выбирает наиболее вероятную цену из списка кандидатов."""
    if not prices:
        return None
    # Сначала high-priority
    high = [p for _, p, pr in prices if pr == 'high']
    pool = high if high else [p for _, p, pr in prices]
    if not pool:
        return None
    # Наиболее частая цена
    most_common = Counter(pool).most_common(1)[0][0]
    return most_common


# ─────────────────────────────────────────────────────────────
# МЕТОД 1 — Внутренний API Ozon
# ─────────────────────────────────────────────────────────────

def _via_ozon_api(url: str) -> Optional[float]:
    """
    Пробует получить цену через внутренний JSON-API Ozon.
    Этот API не требует JS-рендеринга — работает быстро.
    """
    m = re.search(r'ozon\.ru(/product/[^?#]+)', url)
    if not m:
        return None

    path = m.group(1).rstrip('/') + '/'
    api_url = f'https://www.ozon.ru/api/composer-api.bx/page/json/v2?url={path}'
    print(f'     🔌 Уровень 1 — Ozon API: {api_url[:75]}')

    html, err = scrape_url(
        url=api_url,
        render_js=False,
        country_code='ru',
        retry_count=2,
        retry_delay=5.0,
        timeout=30,
    )

    if err or not html:
        print(f'     ℹ️  API недоступен: {(err or "пусто")[:60]}')
        return None

    try:
        data = json.loads(html)
    except json.JSONDecodeError:
        print('     ℹ️  API вернул не JSON')
        return None

    prices = _collect_prices(data)
    price = _best_price(prices)
    if price:
        print(f'     ✅ API нашёл цену: {price:,.0f} ₽')
    return price


# ─────────────────────────────────────────────────────────────
# МЕТОД 2 — HTML с JS-рендерингом
# ─────────────────────────────────────────────────────────────

def _via_html_render(url: str) -> Tuple[Optional[float], str, bool]:
    """
    Получает страницу с JS-рендерингом и ищет цену в:
    1. JSON внутри <script id="*NUXT*"> / <script type="application/json">
    2. window.__NUXT__ = {...} в script-тегах
    3. Regex по сырому HTML — паттерны "finalPrice":45990
    4. JSON-LD (Schema.org) на всякий случай

    Возвращает (цена, название, наличие).
    """
    print('     🌐 Уровень 2 — HTML-рендеринг...')

    html, err = scrape_url(
        url=url,
        render_js=True,
        country_code='ru',
        retry_count=3,
        retry_delay=10.0,
        timeout=75,
    )

    if err or not html:
        print(f'     ❌ Рендер не удался: {(err or "пусто")[:80]}')
        return None, '', False

    print(f'     📄 Получено {len(html):,} символов')
    soup = BeautifulSoup(html, 'lxml')
    price = None

    # ── 2a: script теги с JSON ────────────────────────────
    for tag in soup.find_all('script'):
        tag_id  = tag.get('id', '')
        tag_type = tag.get('type', '')
        raw = tag.string or ''

        is_json_tag = (
            re.search(r'NUXT|STATE|DATA|NEXT', tag_id, re.I) or
            tag_type == 'application/json'
        )
        is_window_var = 'window.__' in raw and len(raw) > 300

        if is_window_var:
            m = re.search(r'window\.__\w+\s*=\s*(\{.+)', raw, re.DOTALL)
            if m:
                raw = m.group(1).rstrip(';\n')

        if (is_json_tag or is_window_var) and len(raw) > 100:
            try:
                data = json.loads(raw)
                prices = _collect_prices(data)
                p = _best_price(prices)
                if p:
                    print(f'     ✅ Script JSON [{tag_id or tag_type}] → {p:,.0f} ₽')
                    price = p
                    break
            except Exception:
                continue

    # ── 2b: Regex по сырому HTML ──────────────────────────
    if price is None:
        high_pattern = (
            r'"(?:finalPrice|cardPrice|sellPrice|salePrice|'
            r'discountedPrice|priceWithCard|sellingPrice)"\s*:\s*(\d{3,7}(?:\.\d+)?)'
        )
        med_pattern = r'"(?:price|currentPrice|minPrice)"\s*:\s*(\d{3,7}(?:\.\d+)?)'

        candidates: List[float] = []
        for pat, weight in [(high_pattern, 3), (med_pattern, 1)]:
            for m in re.finditer(pat, html):
                try:
                    p = float(m.group(1))
                    if 50 <= p <= 10_000_000:
                        candidates.extend([p] * weight)
                except ValueError:
                    pass

        if candidates:
            price = Counter(candidates).most_common(1)[0][0]
            print(f'     ✅ Regex по HTML → {price:,.0f} ₽')

    # ── 2c: JSON-LD fallback ──────────────────────────────
    if price is None:
        for sc in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(sc.string or '')
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get('@type') in ('Product', 'Offer'):
                        offers = item.get('offers', {})
                        pv = None
                        if isinstance(offers, dict):
                            pv = offers.get('price') or offers.get('lowPrice')
                        elif isinstance(offers, list) and offers:
                            pv = offers[0].get('price')
                        if pv:
                            p = _to_price(str(pv))
                            if p:
                                print(f'     ✅ JSON-LD → {p:,.0f} ₽')
                                price = p
                                break
            except Exception:
                continue

    # ── Название и наличие ────────────────────────────────
    name = ''
    h1 = soup.find('h1')
    if h1:
        name = h1.get_text(strip=True)[:300]
    if not name:
        og = soup.find('meta', property='og:title')
        if og:
            name = og.get('content', '')[:300]

    in_stock = True
    for sig in ('нет в наличии', 'товар недоступен', 'нет на складе', 'out of stock'):
        if sig in html.lower():
            in_stock = False
            break

    return price, name, in_stock


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Ozon.
    Пробует внутренний API (быстро), затем HTML-рендеринг (медленно).
    """
    result = {'price': None, 'name': '', 'in_stock': False, 'error': None}

    print(f'   🔵 OZON: {url[:65]}')

    # Уровень 1 — быстрый API
    price = _via_ozon_api(url)

    # Уровень 2 — медленный HTML-рендеринг
    if price is None:
        print('     🔄 Переходим на HTML-рендеринг...')
        price, name, in_stock = _via_html_render(url)
        result['name']     = name
        result['in_stock'] = in_stock
    else:
        result['in_stock'] = True  # Если цена есть — в наличии

    result['price'] = price

    if price is None:
        result['error'] = (
            'Цена Ozon не найдена обоими методами. '
            'Попробуем снова через 3 часа.'
        )
    else:
        print(f'   ✅ OZON итог: {price:,.0f} ₽')

    return result


if __name__ == '__main__':
    test = (sys.argv[1] if len(sys.argv) > 1
            else 'https://www.ozon.ru/product/smartfon-apple-iphone-15-pro-256-gb-1236462765/')
    print(f'\n{"="*60}\nТест Ozon\n{"="*60}')
    r = fetch_price(test)
    print(f'\nЦена:     {r["price"]:,.0f} ₽' if r['price'] else '\nЦена:     НЕ НАЙДЕНА')
    print(f'Название: {r["name"][:80]}')
    print(f'Наличие:  {"✅" if r["in_stock"] else "❌"}')
    if r['error']:
        print(f'Ошибка:   {r["error"]}')
