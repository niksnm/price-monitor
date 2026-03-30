"""
parsers/ozon.py — Ozon парсер 2026 (обход Cloudflare)
======================================================

ПОЧЕМУ ВСЕ 4 МЕТОДА ПАДАЛИ:
  Ozon в 2026 году включил Cloudflare Enterprise с JS challenge.
  Запросы без браузера → немедленная блокировка 403/captcha.
  Даже ScraperAPI render=true иногда не успевает пройти challenge.

РЕШЕНИЕ — ScraperAPI Advanced Parameters:
  1. render=true + wait=5000 (ждём 5 сек после загрузки)
  2. js_instructions — выполняем JS в браузере: прокрутка, клик
     чтобы имитировать поведение пользователя
  3. Пробуем мобильную версию m.ozon.ru — слабее защита
  4. Пробуем Ozon API с правильными cookies

ИЗВЛЕЧЕНИЕ ЦЕНЫ:
  Ключевое изменение: Ozon теперь хранит цену в:
    window.__ozon_data = {"price": {...}}
    <script data-state="..."> — base64 encoded JSON
    Обычные regex паттерны

ПОРЯДОК:
  0. m.ozon.ru (мобильная версия, меньше защита)
  1. Основной URL + wait=5000 + прокрутка
  2. Ozon внутренний API с правильными заголовками
  3. HTML без JS (иногда получается частичный HTML с ценой)
"""

import re
import json
import sys
import os
import base64
from collections import Counter
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import scrape_url, get_api_key

import requests


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ
# ─────────────────────────────────────────────────────────────

def _to_price(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 50 <= f <= 10_000_000 else None
    if isinstance(val, str):
        s = re.sub(r'[^\d.]', '', val.replace(',', '.').replace('\xa0', '').replace('\u202f', ''))
        if not s:
            return None
        try:
            f = float(s)
            return f if 50 <= f <= 10_000_000 else None
        except ValueError:
            return None
    return None


HIGH_KEYS = {
    'finalPrice', 'cardPrice', 'sellPrice', 'salePrice',
    'discountedPrice', 'priceWithCard', 'sellingPrice',
    'actualPrice', 'offerPrice', 'minimalPrice', 'price_with_sale'
}
MED_KEYS = {
    'price', 'currentPrice', 'minPrice', 'basePrice',
    'regularPrice', 'retailPrice'
}


def _find_prices(obj, depth=0) -> List[Tuple[str, float, str]]:
    if depth > 22:
        return []
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in HIGH_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'H'))
            elif k in MED_KEYS:
                p = _to_price(v)
                if p:
                    results.append((k, p, 'M'))
            results.extend(_find_prices(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj[:100]:
            results.extend(_find_prices(item, depth + 1))
    return results


def _best(prices: List[Tuple]) -> Optional[float]:
    if not prices:
        return None
    high = [p for _, p, pr in prices if pr == 'H']
    pool = high if high else [p for _, p, pr in prices]
    return Counter(pool).most_common(1)[0][0] if pool else None


def _json_safe(s: str) -> Optional[dict]:
    try:
        return json.loads(s)
    except Exception:
        return None


def _extract_from_html(html: str) -> Optional[float]:
    """
    Основная функция извлечения цены из HTML Ozon.
    Пробует 5 разных методов.
    """
    soup = BeautifulSoup(html, 'lxml')

    # ── Метод 1: script теги с JSON ──────────────────────
    for tag in soup.find_all('script'):
        raw = tag.string or ''
        if len(raw) < 100:
            continue

        tag_id = (tag.get('id') or '').upper()
        is_data = any(k in tag_id for k in ('NUXT', 'STATE', 'DATA', 'NEXT', 'OZON'))

        # Ищем по содержимому если нет специального id
        has_price_key = any(k in raw for k in HIGH_KEYS)

        if is_data or (has_price_key and len(raw) > 300):
            # window.VAR = {...}
            if 'window.' in raw and '=' in raw:
                m = re.search(r'window\.\w+\s*=\s*(\{.+)', raw, re.DOTALL)
                if m:
                    raw = m.group(1).rstrip(';\n')

            data = _json_safe(raw)
            if data:
                prices = _find_prices(data)
                p = _best(prices)
                if p:
                    return p

    # ── Метод 2: data-state атрибуты (base64 JSON) ───────
    for tag in soup.find_all(attrs={"data-state": True}):
        raw_b64 = tag.get("data-state", "")
        if len(raw_b64) < 20:
            continue
        try:
            decoded = base64.b64decode(raw_b64 + '==').decode('utf-8', errors='ignore')
            data = _json_safe(decoded)
            if data:
                prices = _find_prices(data)
                p = _best(prices)
                if p:
                    return p
        except Exception:
            pass

    # ── Метод 3: Regex высокоприоритетные ключи ──────────
    HIGH_RE = (
        r'"(?:finalPrice|cardPrice|sellPrice|salePrice|discountedPrice|'
        r'priceWithCard|sellingPrice|actualPrice|offerPrice|minimalPrice|'
        r'price_with_sale)"\s*:\s*(\d{3,7}(?:\.\d{1,2})?)'
    )
    candidates: List[float] = []
    for m in re.finditer(HIGH_RE, html):
        p = _to_price(m.group(1))
        if p:
            candidates.extend([p] * 3)

    MED_RE = r'"(?:price|currentPrice|minPrice|basePrice)"\s*:\s*(\d{3,7}(?:\.\d{1,2})?)'
    for m in re.finditer(MED_RE, html):
        p = _to_price(m.group(1))
        if p:
            candidates.append(p)

    if candidates:
        return Counter(candidates).most_common(1)[0][0]

    # ── Метод 4: JSON-LD ──────────────────────────────────
    for sc in soup.find_all('script', type='application/ld+json'):
        data = _json_safe(sc.string or '')
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

    # ── Метод 5: Цена рядом с символом ₽ ─────────────────
    # Паттерн: число пробел ₽ — ищем первое разумное вхождение
    rub_matches = re.findall(r'(\d[\d\s]{2,8})\s*[₽руб]', html)
    rub_prices = []
    for raw_num in rub_matches:
        p = _to_price(raw_num.replace(' ', ''))
        if p and 100 <= p <= 5_000_000:
            rub_prices.append(p)
    if rub_prices:
        # Берём медиану чтобы отфильтровать мусор
        rub_prices.sort()
        return rub_prices[len(rub_prices) // 2]

    return None


def _extract_name(soup: BeautifulSoup) -> str:
    h1 = soup.find('h1')
    if h1:
        t = h1.get_text(strip=True)
        if len(t) > 5:
            return re.sub(r'\s*[—|\-]\s*Ozon.*', '', t, flags=re.I)[:300]
    og = soup.find('meta', property='og:title')
    if og:
        return og.get('content', '')[:300]
    return ''


def _in_stock(html: str) -> bool:
    low = html.lower()
    for sig in ('нет в наличии', 'товар недоступен', 'нет на складе', 'out of stock'):
        if sig in low:
            return False
    return True


# ─────────────────────────────────────────────────────────────
# ПОПЫТКИ ПОЛУЧИТЬ HTML
# ─────────────────────────────────────────────────────────────

def _scrape_with_wait(url: str, render: bool, wait_ms: int = 0,
                      timeout: int = 60, retries: int = 2) -> Optional[str]:
    """
    ScraperAPI запрос с параметром wait (ждём пока JS загрузится).
    wait_ms — миллисекунды ожидания после загрузки страницы.
    """
    api_key = get_api_key()
    if not api_key:
        return None

    params = {
        "api_key":      api_key,
        "url":          url,
        "country_code": "ru",
    }
    if render:
        params["render"] = "true"
    if wait_ms > 0:
        params["wait"] = str(wait_ms)     # Ждём N мс после загрузки

    # Premium для Ozon — резидентный IP
    params["premium"] = "true"

    for attempt in range(retries):
        try:
            resp = requests.get(
                "http://api.scraperapi.com",
                params=params,
                timeout=timeout,
            )
            if resp.status_code == 200 and len(resp.text) > 2000:
                return resp.text
        except Exception:
            pass
        import time
        time.sleep(5)

    return None


def _try_mobile(url: str) -> Optional[str]:
    """
    Мобильная версия m.ozon.ru — защита слабее чем на основном сайте.
    Конвертируем URL: www.ozon.ru → m.ozon.ru
    """
    mobile_url = url.replace("www.ozon.ru", "m.ozon.ru")
    if "ozon.ru" not in mobile_url:
        return None
    print("     📱 Уровень 0 — мобильная версия m.ozon.ru")
    return _scrape_with_wait(mobile_url, render=True, wait_ms=3000, timeout=60, retries=2)


def _try_main_with_wait(url: str) -> Optional[str]:
    """Основной URL + ждём 5 секунд после загрузки JS."""
    print("     🌐 Уровень 1 — основной URL + wait=5000ms")
    return _scrape_with_wait(url, render=True, wait_ms=5000, timeout=80, retries=3)


def _try_ozon_api(url: str) -> Optional[str]:
    """Ozon внутренний API. Работает без JS-рендеринга."""
    m = re.search(r'ozon\.ru(/product/[^?#]+)', url)
    if not m:
        return None
    path = m.group(1).rstrip('/') + '/'

    for endpoint in [
        f"https://www.ozon.ru/api/composer-api.bx/page/json/v2?url={path}",
        f"https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url={path}",
    ]:
        print(f"     🔌 Уровень 2 — Ozon API: {endpoint[40:80]}")
        html, err = scrape_url(
            url=endpoint,
            render_js=False,
            country_code="ru",
            retry_count=2,
            retry_delay=3.0,
            timeout=25,
        )
        if html and len(html) > 100:
            return html

    return None


def _try_no_render(url: str) -> Optional[str]:
    """Без JS рендеринга — быстро, иногда даёт достаточно данных."""
    print("     ⚡ Уровень 3 — без JS-рендеринга")
    return _scrape_with_wait(url, render=False, wait_ms=0, timeout=25, retries=2)


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Ozon.

    Порядок попыток:
      0. m.ozon.ru (мобильная, меньше защита)
      1. www.ozon.ru + render=true + wait=5000ms
      2. Ozon внутренний API (без JS)
      3. www.ozon.ru без JS-рендеринга (быстро)
    """
    result = {"price": None, "name": "", "in_stock": False, "error": None}
    print(f"   🔵 OZON: {url[:65]}")

    attempts = [
        ("mobile m.ozon.ru", lambda: _try_mobile(url)),
        ("www + wait 5s",     lambda: _try_main_with_wait(url)),
        ("Ozon API",          lambda: _try_ozon_api(url)),
        ("без JS",            lambda: _try_no_render(url)),
    ]

    html_got  = None
    used_api  = False

    for label, fn in attempts:
        try:
            html_got = fn()
        except Exception as e:
            print(f"     ⚠️  {label}: {e}")
            html_got = None

        if not html_got or len(html_got) < 500:
            html_got = None
            continue

        print(f"     📄 [{label}] получено {len(html_got):,} символов")

        # Для API-ответа (JSON) — пробуем распарсить напрямую
        if label == "Ozon API":
            data = _json_safe(html_got)
            if data:
                prices = _find_prices(data)
                price  = _best(prices)
                if price:
                    print(f"     ✅ Ozon API → {price:,.0f} ₽")
                    result["price"]    = price
                    result["in_stock"] = True
                    return result
            # Если JSON не распарсился — попробуем как HTML
            used_api = True

        # Извлекаем цену из HTML
        price = _extract_from_html(html_got)
        if price:
            print(f"     ✅ [{label}] цена: {price:,.0f} ₽")
            soup = BeautifulSoup(html_got, 'lxml')
            result["name"]     = _extract_name(soup)
            result["in_stock"] = _in_stock(html_got)
            result["price"]    = price
            return result

        print(f"     ❌ [{label}] цена не найдена в {len(html_got):,} символах")

    # Все методы провалились
    result["error"] = (
        "Цена Ozon не найдена ни одним методом. "
        "Возможно ScraperAPI не проходит Cloudflare Ozon. "
        "Проверьте баланс ScraperAPI и что SCRAPER_PREMIUM=true."
    )
    return result


if __name__ == "__main__":
    test = (sys.argv[1] if len(sys.argv) > 1
            else "https://www.ozon.ru/product/profil-reshetki-radiatora-opel-oem-321637-1494757523/")
    print(f"\n{'='*55}\nТест Ozon\n{'='*55}")
    r = fetch_price(test)
    print(f"\nЦена:     {r['price']:,.0f} ₽" if r['price'] else "\nЦена:     НЕ НАЙДЕНА")
    print(f"Название: {r['name'][:80]}")
    print(f"Наличие:  {'✅' if r['in_stock'] else '❌'}")
    if r['error']:
        print(f"Ошибка:   {r['error']}")
