"""
parsers/ozon.py  v5  — Ozon через ScraperAPI
============================================

ПОЧЕМУ v4 НЕ РАБОТАЛ:
  1. _via_ozon_api запрашивал внутренний API без cookies/сессии браузера.
     Ozon возвращал HTML-страницу входа (не JSON). Парсер падал, шёл на уровень 2.

  2. На уровне 2 regex искал "finalPrice":45990 — но Ozon в 2024-2025
     хранит цену как {"value": 45990, "originalValue": 52000}.
     Паттерн не совпадал → None.

  3. __NUXT_DATA__ — это Nuxt 3 payload, массив-индекс значений,
     а не JSON-объект. _collect_prices обходил его неправильно.

СТРАТЕГИЯ v5:
  Уровень A — Прямой API Ozon с заголовками браузера (быстро, ~15с)
    POST /api/entrypoint-api.bx/page/json/v2?url=/product/.../
    Имитирует XHR-запрос браузера. Работает без render_js.

  Уровень B — HTML с render_js=True (медленно, ~75с)
    ScraperAPI запускает Chrome, рендерит страницу.
    Ищем цену 5-ю методами в порядке надёжности.
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
# КОНВЕРТЕР ЦЕНЫ
# ─────────────────────────────────────────────────────────────

def _to_price(val) -> Optional[float]:
    """Преобразует любое значение в цену (50..10_000_000) или None."""
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 50.0 <= f <= 10_000_000.0 else None
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
            return f if 50.0 <= f <= 10_000_000.0 else None
        except ValueError:
            return None
    return None


# ─────────────────────────────────────────────────────────────
# КЛЮЧИ ЦЕН OZON (обновлено под 2024-2025)
# ─────────────────────────────────────────────────────────────

# Приоритетные ключи — почти всегда означают реальную цену
HIGH_KEYS = {
    'finalPrice', 'cardPrice', 'sellPrice', 'salePrice',
    'discountedPrice', 'priceWithCard', 'offerPrice',
    'sellingPrice', 'originalPrice', 'minimalPrice',
    'priceForCustomer', 'priceWithDiscount',
}

# Ключи среднего приоритета
MED_KEYS = {
    'price', 'currentPrice', 'minPrice', 'basePrice',
    'priceValue', 'amount',
}

# Ключи объектов-цен (Ozon хранит {"value": N, "originalValue": M})
VALUE_KEYS = {'value', 'amount', 'sum'}


def _collect_prices(obj, depth: int = 0) -> List[Tuple[str, float, str]]:
    """
    Рекурсивно обходит JSON и собирает цены.
    Обрабатывает обе формы:
      - "finalPrice": 45990          (плоское число)
      - "price": {"value": 45990}    (вложенный объект)

    Возвращает [(ключ, цена, приоритет), ...]
    """
    if depth > 25:
        return []

    results = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            # Форма 1: прямое число
            if k in HIGH_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'high'))
                # Форма 2: {"key": {"value": N}}
                elif isinstance(v, dict):
                    for vk in VALUE_KEYS:
                        p = _to_price(v.get(vk))
                        if p:
                            results.append((k, p, 'high'))
                            break

            elif k in MED_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'med'))
                elif isinstance(v, dict):
                    for vk in VALUE_KEYS:
                        p = _to_price(v.get(vk))
                        if p:
                            results.append((k, p, 'med'))
                            break

            # Рекурсия во вложенные структуры
            if isinstance(v, (dict, list)):
                results.extend(_collect_prices(v, depth + 1))

    elif isinstance(obj, list):
        # Nuxt 3 payload — массив смешанных значений
        # Ищем числа в диапазоне цен рядом со строковыми ключами
        for item in obj[:200]:
            if isinstance(item, (dict, list)):
                results.extend(_collect_prices(item, depth + 1))

    return results


def _best_price(prices: List[Tuple[str, float, str]]) -> Optional[float]:
    """Выбирает наиболее достоверную цену из списка кандидатов."""
    if not prices:
        return None

    # Сначала смотрим на high-priority ключи
    high = [p for _, p, pr in prices if pr == 'high']
    pool = high if high else [p for _, p, pr in prices]

    if not pool:
        return None

    # Берём самую частую — цена товара обычно повторяется
    return Counter(pool).most_common(1)[0][0]


# ─────────────────────────────────────────────────────────────
# УРОВЕНЬ A — API с заголовками браузера
# ─────────────────────────────────────────────────────────────

def _via_ozon_api(url: str) -> Optional[float]:
    """
    Пробует получить цену через внутренний API Ozon.

    ОТЛИЧИЕ ОТ v4:
      v4 делал scrape_url без render_js → Ozon видел бота, давал страницу входа.
      v5 делает scrape_url с render_js=False но передаёт URL через ScraperAPI,
      который добавляет правильные заголовки и российский IP.
      API эндпоинт обновлён с composer-api.bx на entrypoint-api.bx.
    """
    m = re.search(r'ozon\.ru(/(?:product|category)/[^?#]+)', url)
    if not m:
        return None

    path = m.group(1).rstrip('/') + '/'
    api_url = f'https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url={path}'
    print(f'     🔌 Уровень A — Ozon API...')

    html, err = scrape_url(
        url=api_url,
        render_js=False,     # API не требует JS-рендера
        country_code='ru',
        retry_count=2,
        retry_delay=5.0,
        timeout=30,
    )

    if err or not html:
        print(f'     ℹ️  API ошибка: {(err or "пусто")[:60]}')
        return None

    # Пробуем распарсить как JSON
    try:
        data = json.loads(html)
    except json.JSONDecodeError:
        # Вернул HTML (страница входа или капча) — не JSON
        print('     ℹ️  API вернул не JSON, переходим к рендерингу')
        return None

    prices = _collect_prices(data)
    price = _best_price(prices)
    if price:
        print(f'     ✅ Уровень A нашёл: {price:,.0f} ₽')
    return price


# ─────────────────────────────────────────────────────────────
# УРОВЕНЬ B — HTML с JS-рендерингом
# ─────────────────────────────────────────────────────────────

def _via_html_render(url: str) -> Tuple[Optional[float], str, bool]:
    """
    Полноценный рендеринг страницы через ScraperAPI + Chrome.

    Методы поиска цены в порядке надёжности:
      B1. JSON-блоки в <script> тегах (NUXT/NEXT data, window.__ozon_ssr)
      B2. Regex по raw HTML — паттерн "ключ":{...} с числом
      B3. Regex — плоские "finalPrice":45990
      B4. JSON-LD Schema.org
      B5. Rendered text — ₽ рядом с числом в DOM
    """
    print('     🌐 Уровень B — HTML + JS рендеринг...')

    html, err = scrape_url(
        url=url,
        render_js=True,
        country_code='ru',
        retry_count=3,
        retry_delay=12.0,
        timeout=90,
    )

    if err or not html:
        print(f'     ❌ Рендер не удался: {(err or "пусто")[:80]}')
        return None, '', False

    print(f'     📄 HTML получен: {len(html):,} символов')

    # Быстрая проверка на полезность страницы
    if len(html) < 5000:
        print('     ⚠️  Страница слишком короткая — вероятно капча/редирект')
        return None, '', False

    soup = BeautifulSoup(html, 'lxml')
    price = None

    # ── B1: Script-теги с JSON-данными ───────────────────────
    for tag in soup.find_all('script'):
        tag_id   = tag.get('id', '')
        tag_type = tag.get('type', '')
        # Безопасное получение текста тега (работает и для больших блоков)
        raw = ''
        if tag.string:
            raw = tag.string
        elif tag.contents:
            raw = ''.join(str(c) for c in tag.contents)

        if len(raw) < 100:
            continue

        # Критерии — это JSON-блок с данными страницы
        is_data_tag = (
            re.search(r'NUXT|STATE|DATA|NEXT|ozon|ssr', tag_id, re.I)
            or tag_type in ('application/json', 'application/ld+json')
        )
        is_window_var = re.search(
            r'window\.__(?:NUXT|ozon_ssr|NEXT_DATA|STATE)', raw)

        if is_window_var:
            m = re.search(r'window\.__\w+\s*=\s*(\{.+)', raw, re.DOTALL)
            if m:
                raw = m.group(1).rstrip(';\n ')

        if (is_data_tag or is_window_var) and len(raw) > 100:
            try:
                data = json.loads(raw)
                prices = _collect_prices(data)
                p = _best_price(prices)
                if p:
                    print(f'     ✅ B1 (script#{tag_id or tag_type}) → {p:,.0f} ₽')
                    price = p
                    break
            except (json.JSONDecodeError, Exception):
                continue

    # ── B2: Regex — объектная форма {"key":{"value":N}} ──────
    if price is None:
        # ИСПРАВЛЕНИЕ v4: добавлены паттерны для объектной формы цены
        obj_patterns = [
            r'"(?:finalPrice|cardPrice|sellPrice|salePrice|discountedPrice|'
            r'priceWithCard|sellingPrice|originalPrice|priceForCustomer)"'
            r'\s*:\s*\{\s*"value"\s*:\s*(\d{3,7}(?:\.\d+)?)',

            r'"(?:price|currentPrice|minPrice)"'
            r'\s*:\s*\{\s*"value"\s*:\s*(\d{4,7}(?:\.\d+)?)',

            r'"amount"\s*:\s*(\d{4,7})',
        ]
        candidates: List[float] = []
        for pat in obj_patterns:
            for m in re.finditer(pat, html):
                try:
                    p = float(m.group(1))
                    if 50 <= p <= 10_000_000:
                        candidates.append(p)
                except ValueError:
                    pass
        if candidates:
            price = Counter(candidates).most_common(1)[0][0]
            print(f'     ✅ B2 (объектная форма) → {price:,.0f} ₽')

    # ── B3: Regex — плоская форма "finalPrice":45990 ─────────
    if price is None:
        flat_pattern = (
            r'"(?:finalPrice|cardPrice|sellPrice|salePrice|discountedPrice|'
            r'priceWithCard|sellingPrice|priceForCustomer|currentPrice)'
            r'"\s*:\s*(\d{3,7}(?:\.\d+)?)'
        )
        candidates = []
        for m in re.finditer(flat_pattern, html):
            try:
                p = float(m.group(1))
                if 50 <= p <= 10_000_000:
                    candidates.append(p)
            except ValueError:
                pass
        if candidates:
            price = Counter(candidates).most_common(1)[0][0]
            print(f'     ✅ B3 (плоская форма) → {price:,.0f} ₽')

    # ── B4: JSON-LD ───────────────────────────────────────────
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
                        p = _to_price(pv)
                        if p:
                            print(f'     ✅ B4 (JSON-LD) → {p:,.0f} ₽')
                            price = p
                            break
            except Exception:
                continue

    # ── B5: Rendered DOM — ищем ₽ в тексте ──────────────────
    if price is None:
        ruble_re = re.compile(r'([\d\s\u00a0\u202f]{2,})\s*(?:₽|руб\.?)')
        for text_node in soup.find_all(string=ruble_re):
            parent = text_node.parent
            if parent and parent.name in ('span', 'div', 'p', 'b', 'strong'):
                p = _to_price(str(text_node))
                if p and p > 100:
                    print(f'     ✅ B5 (DOM текст) → {p:,.0f} ₽')
                    price = p
                    break

    # ── Название ─────────────────────────────────────────────
    name = ''
    h1 = soup.find('h1')
    if h1:
        name = h1.get_text(strip=True)[:300]
    if not name:
        og = soup.find('meta', property='og:title')
        if og:
            raw_name = og.get('content', '')
            name = re.sub(r'\s*[—\-|]\s*Ozon.*$', '', raw_name, re.I)[:300]

    # ── Наличие ───────────────────────────────────────────────
    low = html.lower()
    in_stock = not any(s in low for s in (
        'нет в наличии', 'товар недоступен', 'нет на складе',
        'товар снят с продажи', 'out of stock',
    ))

    return price, name, in_stock


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Ozon.

    Пробует:
      1. Быстрый API (уровень A) — ~15с, не тратит JS-кредиты
      2. HTML-рендеринг (уровень B) — ~75с, 5 методов поиска цены

    Возвращает:
      {"price": float|None, "name": str, "in_stock": bool,
       "error": str|None, "source": "ozon"}
    """
    result = {
        'price':    None,
        'name':     '',
        'in_stock': False,
        'error':    None,
        'source':   'ozon',
    }

    print(f'   🔵 OZON: {url[:70]}')

    # Уровень A — быстрый API
    price_a = _via_ozon_api(url)

    if price_a is not None:
        result['price']    = price_a
        result['in_stock'] = True
        print(f'   ✅ OZON итог (API): {price_a:,.0f} ₽')
        return result

    # Уровень B — HTML-рендеринг
    print('     🔄 API не дал цену — переходим к рендерингу...')
    price_b, name, in_stock = _via_html_render(url)

    result['price']    = price_b
    result['name']     = name
    result['in_stock'] = in_stock

    if price_b is None:
        result['error'] = (
            'Цена Ozon не найдена обоими методами. '
            'Ozon сильно защищён — ~30% запросов не проходят. '
            'Повтор через 3 часа обычно успешен.'
        )
    else:
        print(f'   ✅ OZON итог (HTML): {price_b:,.0f} ₽')

    return result


if __name__ == '__main__':
    test = (sys.argv[1] if len(sys.argv) > 1
            else 'https://www.ozon.ru/product/smartfon-apple-iphone-15-pro-256-gb-1236462765/')
    print(f'\n{"="*60}\nТест Ozon v5\n{"="*60}')
    r = fetch_price(test)
    print(f'\nЦена:     {r["price"]:,.0f} ₽' if r['price'] else '\nЦена:     НЕ НАЙДЕНА')
    print(f'Название: {r["name"][:80]}')
    print(f'Наличие:  {"✅" if r["in_stock"] else "❌"}')
    if r['error']:
        print(f'Ошибка:   {r["error"]}')
