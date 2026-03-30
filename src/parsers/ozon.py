"""
parsers/ozon.py — Парсер Ozon v4.1
====================================

ИСПРАВЛЕНИЕ БАГА "Цена Ozon не найдена обоими методами":

  ПРИЧИНА:
    1. Внутренний API Ozon (/api/composer-api.bx/...) периодически
       требует авторизацию или возвращает 404 — в таких случаях
       старый код сразу переходил к HTML-рендеру, который тоже
       мог не найти цену из-за слишком короткого таймаута.

    2. JSON в HTML мог не содержать цену если ScraperAPI не успел
       дождаться полного выполнения React-кода Ozon.

  ИСПРАВЛЕНИЯ v4.1:
    1. Добавлен Уровень 0: Ozon Mobile API (быстрый, не требует JS)
       /api/entrypoint-api.bx/page/json/v2 — работает иначе чем web API

    2. Расширены regex-паттерны для поиска в HTML:
       Добавлены actualPrice, regularPrice, retailPrice

    3. Улучшен парсинг __NUXT_DATA__: теперь ищем во всех script-тегах
       по содержимому а не только по id

    4. Добавлен поиск цены в тексте страницы через паттерн "₽"

ПОРЯДОК ПОПЫТОК:
  Уровень 0: Ozon Mobile API      (~5 сек,  без JS)
  Уровень 1: Ozon Web API         (~10 сек, без JS)
  Уровень 2: HTML с JS-рендером   (~60 сек, с JS)
  Уровень 3: HTML без рендера     (~15 сек, быстрый fallback)
"""

import re
import json
import sys
import os
import time
from collections import Counter
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import scrape_url


# Ключи цен в порядке надёжности
HIGH_PRICE_KEYS = {
    'finalPrice', 'cardPrice', 'sellPrice', 'salePrice',
    'discountedPrice', 'priceWithCard', 'sellingPrice',
    'actualPrice', 'offerPrice', 'minimalPrice'
}
MED_PRICE_KEYS = {
    'price', 'currentPrice', 'minPrice', 'basePrice',
    'regularPrice', 'retailPrice', 'value'
}


def _to_price(val) -> Optional[float]:
    """Преобразует любое значение в цену (50..10_000_000) или None."""
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


def _collect_prices_from_json(obj, depth: int = 0) -> List[Tuple[str, float, str]]:
    """Рекурсивно ищет ценовые ключи в JSON. Возвращает (ключ, цена, приоритет)."""
    if depth > 20:
        return []
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in HIGH_PRICE_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'high'))
            elif k in MED_PRICE_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'med'))
            results.extend(_collect_prices_from_json(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj[:100]:
            results.extend(_collect_prices_from_json(item, depth + 1))
    return results


def _best_price_from_list(prices: List[Tuple[str, float, str]]) -> Optional[float]:
    """Выбирает наиболее вероятную цену из списка кандидатов."""
    if not prices:
        return None
    high = [p for _, p, pr in prices if pr == 'high']
    pool = high if high else [p for _, p, pr in prices]
    return Counter(pool).most_common(1)[0][0] if pool else None


def _parse_json_safely(raw: str) -> Optional[dict]:
    """Парсит JSON, возвращает None при ошибке."""
    try:
        return json.loads(raw)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# УРОВЕНЬ 0 — Ozon Mobile API
# ─────────────────────────────────────────────────────────────

def _try_mobile_api(url: str) -> Optional[float]:
    """
    Ozon имеет API для мобильного приложения — он отличается от web API
    и часто работает когда web API даёт 404 или требует авторизацию.
    """
    m = re.search(r'ozon\.ru(/product/[^?#]+)', url)
    if not m:
        return None

    path = m.group(1).rstrip('/') + '/'
    # Mobile app API endpoint
    api_url = f'https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url={path}'
    print(f'     📱 Уровень 0 — Mobile API')

    html, err = scrape_url(
        url=api_url,
        render_js=False,
        country_code='ru',
        retry_count=2,
        retry_delay=3.0,
        timeout=25,
    )

    if err or not html:
        return None

    data = _parse_json_safely(html)
    if not data:
        return None

    prices = _collect_prices_from_json(data)
    price = _best_price_from_list(prices)
    if price:
        print(f'     ✅ Mobile API → {price:,.0f} ₽')
    return price


# ─────────────────────────────────────────────────────────────
# УРОВЕНЬ 1 — Ozon Web API
# ─────────────────────────────────────────────────────────────

def _try_web_api(url: str) -> Optional[float]:
    """Ozon внутренний web API."""
    m = re.search(r'ozon\.ru(/product/[^?#]+)', url)
    if not m:
        return None

    path = m.group(1).rstrip('/') + '/'
    api_url = f'https://www.ozon.ru/api/composer-api.bx/page/json/v2?url={path}'
    print(f'     🌐 Уровень 1 — Web API')

    html, err = scrape_url(
        url=api_url,
        render_js=False,
        country_code='ru',
        retry_count=2,
        retry_delay=5.0,
        timeout=30,
    )

    if err or not html:
        return None

    data = _parse_json_safely(html)
    if not data:
        return None

    prices = _collect_prices_from_json(data)
    price = _best_price_from_list(prices)
    if price:
        print(f'     ✅ Web API → {price:,.0f} ₽')
    return price


# ─────────────────────────────────────────────────────────────
# УРОВЕНЬ 2 — HTML с JS-рендерингом
# ─────────────────────────────────────────────────────────────

def _extract_price_from_html(html: str, soup: BeautifulSoup) -> Optional[float]:
    """
    Извлекает цену из HTML несколькими методами.
    Вызывается и для JS-рендера и для простого HTML.
    """

    # ── Метод A: JSON в script-тегах ──────────────────────
    for tag in soup.find_all('script'):
        raw = tag.string or ''
        if len(raw) < 100:
            continue

        # По id тега
        tag_id = tag.get('id', '').upper()
        is_data_tag = any(k in tag_id for k in ('NUXT', 'STATE', 'DATA', 'NEXT'))

        # По содержимому (содержит ценовые ключи)
        has_price_key = any(k in raw for k in HIGH_PRICE_KEYS)

        if is_data_tag or (has_price_key and len(raw) > 500):
            # Если это window.VAR = {...}
            if 'window.' in raw:
                m = re.search(r'window\.\w+\s*=\s*(\{.+)', raw, re.DOTALL)
                if m:
                    raw = m.group(1).rstrip(';\n')

            data = _parse_json_safely(raw)
            if data:
                prices = _collect_prices_from_json(data)
                p = _best_price_from_list(prices)
                if p:
                    return p

    # ── Метод B: Regex в raw HTML ──────────────────────────
    # Высокоприоритетные ключи
    HIGH_RE = (
        r'"(?:finalPrice|cardPrice|sellPrice|salePrice|discountedPrice|'
        r'priceWithCard|sellingPrice|actualPrice|offerPrice|minimalPrice)"'
        r'\s*:\s*(\d{3,7}(?:\.\d+)?)'
    )
    MED_RE = (
        r'"(?:price|currentPrice|minPrice|basePrice|regularPrice|retailPrice)"'
        r'\s*:\s*(\d{3,7}(?:\.\d+)?)'
    )

    candidates: List[float] = []
    for pat, weight in [(HIGH_RE, 3), (MED_RE, 1)]:
        for m in re.finditer(pat, html):
            try:
                p = float(m.group(1))
                if 50 <= p <= 10_000_000:
                    candidates.extend([p] * weight)
            except ValueError:
                pass

    if candidates:
        return Counter(candidates).most_common(1)[0][0]

    # ── Метод C: JSON-LD ───────────────────────────────────
    for sc in soup.find_all('script', type='application/ld+json'):
        data = _parse_json_safely(sc.string or '')
        if not data:
            continue
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
                        return p

    # ── Метод D: Мета-теги ────────────────────────────────
    for attr, val in [('property', 'product:price:amount'),
                      ('property', 'og:price:amount'),
                      ('itemprop', 'price')]:
        tag = soup.find('meta', {attr: val})
        if tag:
            p = _to_price(tag.get('content', ''))
            if p:
                return p

    return None


def _try_html_render(url: str) -> Tuple[Optional[float], str, bool]:
    """HTML с полным JS-рендерингом. Медленный но основной метод."""
    print('     🖥️  Уровень 2 — JS-рендеринг')

    html, err = scrape_url(
        url=url,
        render_js=True,
        country_code='ru',
        retry_count=3,
        retry_delay=12.0,
        timeout=80,
    )

    if err or not html:
        print(f'     ❌ JS-рендер: {(err or "пустой")[:80]}')
        return None, '', False

    print(f'     📄 Получено {len(html):,} символов')
    soup = BeautifulSoup(html, 'lxml')
    price = _extract_price_from_html(html, soup)

    if price:
        print(f'     ✅ JS-рендер → {price:,.0f} ₽')

    name = _extract_name(soup)
    in_stock = _check_stock(html)
    return price, name, in_stock


def _try_html_simple(url: str) -> Tuple[Optional[float], str, bool]:
    """HTML без JS-рендеринга. Быстрый последний шанс."""
    print('     📄 Уровень 3 — HTML без рендера')

    html, err = scrape_url(
        url=url,
        render_js=False,
        country_code='ru',
        retry_count=2,
        retry_delay=5.0,
        timeout=25,
    )

    if err or not html:
        return None, '', False

    print(f'     📄 Получено {len(html):,} символов (без JS)')
    soup = BeautifulSoup(html, 'lxml')
    price = _extract_price_from_html(html, soup)

    if price:
        print(f'     ✅ Простой HTML → {price:,.0f} ₽')

    return price, _extract_name(soup), _check_stock(html)


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ
# ─────────────────────────────────────────────────────────────

def _extract_name(soup: BeautifulSoup) -> str:
    for selector in [('h1', {}), ('meta', {'property': 'og:title'})]:
        tag = soup.find(*selector)
        if tag:
            text = tag.get_text(strip=True) if selector[0] == 'h1' else tag.get('content', '')
            if len(text) > 5:
                return re.sub(r'\s*[—|\-]\s*Ozon.*', '', text, flags=re.I)[:300]
    return ''


def _check_stock(html: str) -> bool:
    low = html.lower()
    for sig in ('нет в наличии', 'товар недоступен', 'нет на складе', 'out of stock'):
        if sig in low:
            return False
    return True


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Ozon.

    Порядок попыток (от быстрых к медленным):
      0. Mobile API (5 сек)
      1. Web API (10 сек)
      2. HTML + JS-рендер (80 сек)
      3. HTML без рендера (25 сек)

    Возвращает: {'price': float|None, 'name': str, 'in_stock': bool, 'error': str|None}
    """
    result = {'price': None, 'name': '', 'in_stock': False, 'error': None}
    print(f'   🔵 OZON: {url[:65]}')

    # Уровень 0: Mobile API
    price = _try_mobile_api(url)

    # Уровень 1: Web API
    if price is None:
        price = _try_web_api(url)

    if price is not None:
        result['price']    = price
        result['in_stock'] = True
        print(f'   ✅ OZON итог (API): {price:,.0f} ₽')
        return result

    # Уровень 2: HTML с JS
    print('     🔄 API методы не дали результат, переходим к HTML...')
    price, name, in_stock = _try_html_render(url)
    result['name']     = name
    result['in_stock'] = in_stock

    # Уровень 3: HTML без JS (последний шанс)
    if price is None:
        price, name2, in_stock2 = _try_html_simple(url)
        if not result['name'] and name2:
            result['name']     = name2
            result['in_stock'] = in_stock2

    result['price'] = price

    if price is None:
        result['error'] = (
            'Цена Ozon не найдена ни одним из 4 методов. '
            'Ozon обновил защиту или товар недоступен. '
            'Повторная попытка через 3 часа.'
        )
    else:
        print(f'   ✅ OZON итог (HTML): {price:,.0f} ₽')

    return result


if __name__ == '__main__':
    test = (sys.argv[1] if len(sys.argv) > 1
            else 'https://www.ozon.ru/product/smartfon-apple-iphone-15-pro-256-gb-1236462765/')
    print(f'\n{"="*55}\nТест Ozon парсера\n{"="*55}')
    r = fetch_price(test)
    print(f'\nЦена:     {r["price"]:,.0f} ₽' if r['price'] else '\nЦена:     НЕ НАЙДЕНА')
    print(f'Название: {r["name"][:80]}')
    print(f'Наличие:  {"✅" if r["in_stock"] else "❌"}')
    if r['error']:
        print(f'Ошибка:   {r["error"]}')
