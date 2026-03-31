"""
parsers/ozon.py — Ozon 2026 (ScraperAPI autoparse + mobile)
============================================================

ПОЧЕМУ ПРЕДЫДУЩИЕ МЕТОДЫ НЕ РАБОТАЛИ:
  Ozon включил Cloudflare Turnstile в 2026.
  render=true сам по себе не проходит Turnstile challenge.
  wait=5000 не помогает — challenge требует браузерных событий.

НОВЫЕ МЕТОДЫ:

  1. ScraperAPI autoparse=true
     Специальный режим ScraperAPI для e-commerce сайтов.
     Параметр autoparse=true включает их AI-экстрактор —
     возвращает структурированный JSON с ценой, не HTML.
     URL: api.scraperapi.com?autoparse=true&url=ozon.ru/...

  2. device_type=mobile
     Мобильная версия Ozon имеет ДРУГОЙ JS-код и слабее защита.
     ScraperAPI параметр: device_type=mobile

  3. m.ozon.ru напрямую
     Мобильный домен — другой CDN, другая защита.

  4. Ozon API с session cookie
     Сначала берём cookie с главной, потом запрашиваем товар.
"""

import re
import json
import sys
import os
import base64
import requests
from collections import Counter
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import get_api_key

import time


SCRAPERAPI_BASE = "http://api.scraperapi.com"


def _to_price(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 50 <= f <= 10_000_000 else None
    if isinstance(val, str):
        s = re.sub(r'[^\d.]', '',
                   val.replace(',', '.').replace('\xa0', '').replace('\u202f', ''))
        try:
            f = float(s)
            return f if 50 <= f <= 10_000_000 else None
        except ValueError:
            return None
    return None


HIGH_KEYS = {
    'finalPrice', 'cardPrice', 'sellPrice', 'salePrice',
    'discountedPrice', 'priceWithCard', 'sellingPrice',
    'actualPrice', 'offerPrice', 'minimalPrice', 'price_with_sale',
}
MED_KEYS = {'price', 'currentPrice', 'minPrice', 'basePrice', 'regularPrice'}


def _find_prices(obj, depth=0) -> List[Tuple[str, float, str]]:
    if depth > 22:
        return []
    res = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in HIGH_KEYS:
                p = _to_price(v)
                if p:
                    res.append((k, p, 'H'))
            elif k in MED_KEYS:
                p = _to_price(v)
                if p:
                    res.append((k, p, 'M'))
            res.extend(_find_prices(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj[:100]:
            res.extend(_find_prices(item, depth + 1))
    return res


def _best(prices: List[Tuple]) -> Optional[float]:
    if not prices:
        return None
    high = [p for _, p, pr in prices if pr == 'H']
    pool = high if high else [p for _, p, pr in prices]
    return Counter(pool).most_common(1)[0][0] if pool else None


def _safe_json(s) -> Optional[dict]:
    try:
        return json.loads(s)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# МЕТОД 1 — ScraperAPI autoparse
# ─────────────────────────────────────────────────────────────

def _try_autoparse(url: str) -> Optional[float]:
    """
    ScraperAPI autoparse=true — специальный AI-режим для e-commerce.
    Возвращает структурированный JSON с полями name, price, availability.

    Документация: https://docs.scraperapi.com/making-requests/auto-parse
    """
    api_key = get_api_key()
    if not api_key:
        return None

    print("     🤖 Метод 1: ScraperAPI autoparse=true")

    params = {
        "api_key":      api_key,
        "url":          url,
        "autoparse":    "true",
        "country_code": "ru",
        "premium":      "true",
    }

    for attempt in range(2):
        try:
            resp = requests.get(SCRAPERAPI_BASE, params=params, timeout=60)
            if resp.status_code == 200:
                # autoparse возвращает JSON напрямую
                data = _safe_json(resp.text)
                if data:
                    # Стандартные поля autoparse
                    price = (
                        _to_price(data.get("price"))
                        or _to_price(data.get("sale_price"))
                        or _to_price(data.get("original_price"))
                    )
                    if price:
                        print(f"     ✅ autoparse → {price:,.0f} ₽")
                        return price

                    # Если autoparse вернул HTML внутри JSON
                    content = data.get("html") or data.get("body") or ""
                    if content and len(content) > 1000:
                        p = _extract_from_html(content)
                        if p:
                            print(f"     ✅ autoparse(html) → {p:,.0f} ₽")
                            return p
        except Exception as e:
            print(f"     ⚠️  autoparse попытка {attempt+1}: {e}")
        time.sleep(3)

    return None


# ─────────────────────────────────────────────────────────────
# МЕТОД 2 — device_type=mobile
# ─────────────────────────────────────────────────────────────

def _try_mobile(url: str) -> Optional[str]:
    """
    ScraperAPI с device_type=mobile — другой fingerprint, другой Ozon код.
    """
    api_key = get_api_key()
    if not api_key:
        return None

    # Конвертируем на мобильный домен
    mobile_url = url.replace("www.ozon.ru", "m.ozon.ru")

    print(f"     📱 Метод 2: mobile {mobile_url[:60]}")

    params = {
        "api_key":      api_key,
        "url":          mobile_url,
        "render":       "true",
        "wait":         "4000",
        "device_type":  "mobile",
        "country_code": "ru",
        "premium":      "true",
    }

    for attempt in range(2):
        try:
            resp = requests.get(SCRAPERAPI_BASE, params=params, timeout=75)
            if resp.status_code == 200 and len(resp.text) > 2000:
                print(f"     📄 mobile: {len(resp.text):,} символов")
                return resp.text
        except Exception:
            pass
        time.sleep(5)

    return None


# ─────────────────────────────────────────────────────────────
# МЕТОД 3 — render=true + wait=8000
# ─────────────────────────────────────────────────────────────

def _try_render_wait(url: str) -> Optional[str]:
    """render=true + ждём 8 секунд — Cloudflare challenge должен пройти."""
    api_key = get_api_key()
    if not api_key:
        return None

    print("     🌐 Метод 3: render + wait=8000ms")

    params = {
        "api_key":      api_key,
        "url":          url,
        "render":       "true",
        "wait":         "8000",
        "country_code": "ru",
        "premium":      "true",
    }

    for attempt in range(3):
        try:
            resp = requests.get(SCRAPERAPI_BASE, params=params, timeout=90)
            if resp.status_code == 200 and len(resp.text) > 3000:
                # Проверяем что это не страница капчи
                if "turnstile" in resp.text.lower() and len(resp.text) < 50000:
                    print(f"     ⚠️  Попытка {attempt+1}: Cloudflare Turnstile challenge")
                    time.sleep(8)
                    continue
                print(f"     📄 render+wait: {len(resp.text):,} символов")
                return resp.text
        except Exception as e:
            print(f"     ⚠️  render+wait попытка {attempt+1}: {e}")
        time.sleep(10)

    return None


# ─────────────────────────────────────────────────────────────
# МЕТОД 4 — Ozon API (без рендера)
# ─────────────────────────────────────────────────────────────

def _try_ozon_api(url: str) -> Optional[str]:
    """Ozon внутренние API эндпоинты."""
    api_key = get_api_key()
    if not api_key:
        return None

    m = re.search(r'ozon\.ru(/product/[^?#]+)', url)
    if not m:
        return None
    path = m.group(1).rstrip('/') + '/'

    endpoints = [
        f"https://www.ozon.ru/api/composer-api.bx/page/json/v2?url={path}",
        f"https://www.ozon.ru/api/entrypoint-api.bx/page/json/v2?url={path}",
    ]

    for endpoint in endpoints:
        print(f"     🔌 Метод 4: Ozon API {endpoint[35:75]}")
        params = {
            "api_key":      api_key,
            "url":          endpoint,
            "render":       "false",
            "country_code": "ru",
            "premium":      "true",
        }
        try:
            resp = requests.get(SCRAPERAPI_BASE, params=params, timeout=30)
            if resp.status_code == 200 and len(resp.text) > 100:
                return resp.text
        except Exception:
            pass
        time.sleep(2)

    return None


# ─────────────────────────────────────────────────────────────
# ИЗВЛЕЧЕНИЕ ЦЕНЫ ИЗ HTML / JSON
# ─────────────────────────────────────────────────────────────

def _extract_from_html(html: str) -> Optional[float]:
    """Извлекает цену из HTML или JSON строки Ozon."""
    if not html or len(html) < 100:
        return None

    # ── Если это JSON (от API) ───────────────────────────
    data = _safe_json(html)
    if data:
        prices = _find_prices(data)
        p = _best(prices)
        if p:
            return p

    soup = BeautifulSoup(html, 'lxml')

    # ── Script теги с JSON ───────────────────────────────
    for tag in soup.find_all('script'):
        raw = tag.string or ''
        if len(raw) < 100:
            continue
        tag_id = (tag.get('id') or '').upper()
        has_key = any(k in raw for k in HIGH_KEYS)
        is_data_tag = any(k in tag_id for k in ('NUXT', 'STATE', 'DATA', 'NEXT'))

        if is_data_tag or (has_key and len(raw) > 300):
            if 'window.' in raw and '=' in raw:
                m = re.search(r'window\.\w+\s*=\s*(\{.+)', raw, re.DOTALL)
                if m:
                    raw = m.group(1).rstrip(';\n')
            d = _safe_json(raw)
            if d:
                prices = _find_prices(d)
                p = _best(prices)
                if p:
                    return p

    # ── data-state (base64) ──────────────────────────────
    for tag in soup.find_all(attrs={"data-state": True}):
        b64 = tag.get("data-state", "")
        if len(b64) < 20:
            continue
        try:
            decoded = base64.b64decode(b64 + '==').decode('utf-8', errors='ignore')
            d = _safe_json(decoded)
            if d:
                prices = _find_prices(d)
                p = _best(prices)
                if p:
                    return p
        except Exception:
            pass

    # ── Regex высокий приоритет ──────────────────────────
    HIGH_PAT = (
        r'"(?:finalPrice|cardPrice|sellPrice|salePrice|discountedPrice|'
        r'priceWithCard|sellingPrice|actualPrice|offerPrice|minimalPrice|'
        r'price_with_sale)"\s*:\s*(\d{3,7}(?:\.\d{1,2})?)'
    )
    candidates: List[float] = []
    for m in re.finditer(HIGH_PAT, html):
        p = _to_price(m.group(1))
        if p:
            candidates.extend([p] * 3)

    MED_PAT = r'"(?:price|currentPrice|minPrice|basePrice)"\s*:\s*(\d{3,7}(?:\.\d{1,2})?)'
    for m in re.finditer(MED_PAT, html):
        p = _to_price(m.group(1))
        if p:
            candidates.append(p)

    if candidates:
        return Counter(candidates).most_common(1)[0][0]

    # ── JSON-LD ──────────────────────────────────────────
    for sc in soup.find_all('script', type='application/ld+json'):
        d = _safe_json(sc.string or '')
        if not d:
            continue
        items = d if isinstance(d, list) else [d]
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
    return not any(s in low for s in (
        'нет в наличии', 'товар недоступен', 'нет на складе', 'out of stock'
    ))


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Ozon.

    Методы по порядку:
      1. autoparse=true (AI-экстрактор ScraperAPI)
      2. device_type=mobile + render
      3. render=true + wait=8000ms
      4. Ozon внутренний API
    """
    result = {"price": None, "name": "", "in_stock": False, "error": None}
    print(f"   🔵 OZON: {url[:65]}")

    price = None
    html_for_meta = None

    # ── Метод 1: autoparse ───────────────────────────────
    price = _try_autoparse(url)

    # ── Метод 2: mobile ──────────────────────────────────
    if price is None:
        html = _try_mobile(url)
        if html:
            html_for_meta = html
            price = _extract_from_html(html)
            if price:
                print(f"     ✅ mobile → {price:,.0f} ₽")

    # ── Метод 3: render + wait ───────────────────────────
    if price is None:
        html = _try_render_wait(url)
        if html:
            html_for_meta = html
            price = _extract_from_html(html)
            if price:
                print(f"     ✅ render+wait → {price:,.0f} ₽")

    # ── Метод 4: Ozon API ─────────────────────────────────
    if price is None:
        html = _try_ozon_api(url)
        if html:
            price = _extract_from_html(html)
            if price:
                print(f"     ✅ Ozon API → {price:,.0f} ₽")

    result["price"] = price

    # Название и наличие из последнего полученного HTML
    if html_for_meta:
        try:
            soup = BeautifulSoup(html_for_meta, 'lxml')
            result["name"]     = _extract_name(soup)
            result["in_stock"] = _in_stock(html_for_meta)
        except Exception:
            result["in_stock"] = price is not None

    if price is None:
        result["error"] = (
            "Цена Ozon не найдена ни одним методом. "
            "Ozon использует Cloudflare Turnstile — "
            "проверьте что SCRAPER_PREMIUM=true в Secrets."
        )
    else:
        print(f"   ✅ OZON итог: {price:,.0f} ₽")

    return result
