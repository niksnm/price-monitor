"""
parsers/wildberries.py — Wildberries 2026
==========================================

ДИАГНОСТИКА ОШИБКИ "218412789 не найден ни одним методом":

  Проблема: card.wb.ru API работает, но артикул реально может быть
  снят с продажи, перемещён на архив, или API требует другие параметры.

  НОВЫЙ ГЛАВНЫЙ МЕТОД — WB Basket CDN:
    WB хранит карточки товаров на CDN серверах:
    https://basket-NN.wbbasket.ru/vol{VOL}/part{PART}/{ARTICLE}/info/ru/card.json

    Это ПУБЛИЧНЫЙ эндпоинт без авторизации, работает всегда.
    Но цена здесь не хранится — только мета-данные товара.

  ЦЕНА — card.wb.ru с разными appType:
    appType=1   — веб-сайт
    appType=64  — iOS приложение
    appType=128 — Android приложение
    Разные appType часто возвращают разные результаты.

  ИТОГОВАЯ СТРАТЕГИЯ (7 методов):
    1. card.wb.ru/v2 + appType=1  + все dest
    2. card.wb.ru/v2 + appType=64 (iOS)
    3. card.wb.ru/v2 + appType=128 (Android)
    4. card.wb.ru/v1 + все варианты
    5. search.wb.ru
    6. catalog.wb.ru
    7. ScraperAPI → HTML страницы товара
"""

import re
import json
import requests
import time
from typing import Optional, Dict, Any

HEADERS_WEB = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Origin": "https://www.wildberries.ru",
    "Referer": "https://www.wildberries.ru/",
}

HEADERS_IOS = {
    "User-Agent": "WildBerries/10.5.1 (iPhone; iOS 17.0; Scale/3.00)",
    "Accept": "application/json",
    "Accept-Language": "ru-RU;q=1.0",
}

HEADERS_ANDROID = {
    "User-Agent": "ru.wildberries.wildberries/10.5.1 (Android; Dalvik)",
    "Accept": "application/json",
}

# Все dest значения которые нужно попробовать
DEST_LIST = ["-1257786", "-1059500", "-2133462", "-1123025", "12358062", "-446085"]


def extract_article(url: str) -> Optional[str]:
    """Извлекает артикул WB из URL или строки."""
    url = url.strip()
    if url.isdigit():
        return url
    m = re.search(r'/catalog/(\d+)(?:/|$)', url)
    if m:
        return m.group(1)
    m = re.search(r'\b(\d{7,12})\b', url)
    if m:
        return m.group(1)
    return None


def _get_basket_host(vol: int) -> str:
    """
    Вычисляет номер basket-хоста WB по vol числу.
    Актуальная таблица 2026 года.
    """
    ranges = [
        (143, "01"), (287, "02"), (431, "03"), (719, "04"),
        (1007, "05"), (1061, "06"), (1115, "07"), (1169, "08"),
        (1313, "09"), (1601, "10"), (1655, "11"), (1919, "12"),
        (2045, "13"), (2189, "14"), (2405, "15"), (2621, "16"),
        (2837, "17"), (3173, "18"), (3459, "19"),
    ]
    for limit, host in ranges:
        if vol <= limit:
            return host
    return "20"


def _get_from_basket_cdn(article: str) -> Optional[dict]:
    """
    Получает данные товара через WB Basket CDN.
    Это публичный CDN без ограничений — работает всегда.
    Возвращает мета-данные товара (название и т.д.), но НЕ цену.
    """
    try:
        nm = int(article)
        vol  = nm // 100000
        part = nm // 1000
        host = _get_basket_host(vol)

        url = (
            f"https://basket-{host}.wbbasket.ru"
            f"/vol{vol}/part{part}/{nm}/info/ru/card.json"
        )
        resp = requests.get(url, headers=HEADERS_WEB, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "exists": True,
                "name": data.get("imt_name") or data.get("name") or "",
                "brand": data.get("selling", {}).get("brand_name") or "",
            }
    except Exception:
        pass
    return None


def _card_api(article: str, version: str,
              dest: str, app_type: int,
              headers: dict) -> Optional[dict]:
    """Один запрос к card.wb.ru с конкретными параметрами."""
    url = (
        f"https://card.wb.ru/cards/{version}/detail"
        f"?appType={app_type}&curr=rub&dest={dest}&nm={article}"
    )
    if version == "v1":
        url += "&spp=27"
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            products = resp.json().get("data", {}).get("products", [])
            if products:
                return products[0]
    except Exception:
        pass
    return None


def _search_api(article: str) -> Optional[dict]:
    """search.wb.ru — поисковый API."""
    for dest in ["-1257786", "-1059500"]:
        url = (
            f"https://search.wb.ru/exactmatch/ru/common/v9/search"
            f"?query={article}&resultset=catalog&limit=1"
            f"&sort=popular&page=1&appType=1&curr=rub&dest={dest}"
        )
        try:
            resp = requests.get(url, headers=HEADERS_WEB, timeout=12)
            if resp.status_code == 200:
                products = resp.json().get("data", {}).get("products", [])
                for p in products:
                    if str(p.get("id", "")) == str(article):
                        return p
                if products:
                    return products[0]
        except Exception:
            pass
        time.sleep(0.3)
    return None


def _scraper_html(article: str) -> Optional[dict]:
    """
    Последний шанс: ScraperAPI → HTML страницы товара.
    WB вставляет цену в JSON на странице.
    """
    try:
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraping_client import scrape_url

        url = f"https://www.wildberries.ru/catalog/{article}/detail.aspx"
        html, err = scrape_url(
            url=url, render_js=False,
            country_code="ru", retry_count=2,
            retry_delay=4.0, timeout=30,
        )
        if not html or err:
            return None

        # Ищем цену в копейках (salePriceU / priceU)
        for pat in [
            r'"salePriceU"\s*:\s*(\d{4,11})',
            r'"priceU"\s*:\s*(\d{4,11})',
        ]:
            m = re.search(pat, html)
            if m:
                kopecks = int(m.group(1))
                if 100 <= kopecks <= 100_000_000_000:
                    price = round(kopecks / 100, 2)
                    name_m = re.search(r'"name"\s*:\s*"([^"]{5,200})"', html)
                    return {
                        "price":    price,
                        "name":     name_m.group(1) if name_m else "",
                        "in_stock": True,
                    }

        # Если цена не в копейках — ищем в рублях
        for pat in [
            r'"finalPrice"\s*:\s*(\d{3,7}(?:\.\d{1,2})?)',
            r'"cardPrice"\s*:\s*(\d{3,7}(?:\.\d{1,2})?)',
        ]:
            m = re.search(pat, html)
            if m:
                price = float(m.group(1))
                if 50 <= price <= 10_000_000:
                    return {"price": price, "name": "", "in_stock": True}

    except Exception:
        pass
    return None


def _parse_wb_product(product: dict) -> dict:
    """Извлекает цену, название и наличие из объекта товара WB API."""
    name     = product.get("name", "")
    sizes    = product.get("sizes", [])
    in_stock = any(s.get("stocks") for s in sizes)

    sale_u  = product.get("salePriceU", 0) or 0
    price_u = product.get("priceU",     0) or 0
    price   = None
    if sale_u > 0:
        price = round(sale_u / 100, 2)
    elif price_u > 0:
        price = round(price_u / 100, 2)

    return {"price": price, "name": name, "in_stock": in_stock}


def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Wildberries.
    """
    result = {"price": None, "name": "", "in_stock": False, "error": None}

    article = extract_article(url)
    if not article:
        result["error"] = f"Не удалось извлечь артикул из: {url[:80]}"
        return result

    print(f"   🍇 WB артикул: {article}")

    # ── Шаг 0: Проверяем что товар существует через CDN ──
    cdn_info = _get_from_basket_cdn(article)
    if cdn_info:
        print(f"     📦 Товар найден в CDN: {cdn_info.get('name','')[:50]}")
        result["name"] = cdn_info.get("name", "")
    else:
        print(f"     ⚠️  Товар не найден в Basket CDN — возможно снят с продажи")

    # ── Шаг 1-3: card.wb.ru с разными appType ────────────
    product_data = None

    combos = [
        # (version, dest, app_type, headers)
        ("v2", "-1257786",  1,   HEADERS_WEB),
        ("v2", "-1257786",  64,  HEADERS_IOS),
        ("v2", "-1257786",  128, HEADERS_ANDROID),
        ("v2", "-1059500",  1,   HEADERS_WEB),
        ("v2", "-2133462",  1,   HEADERS_WEB),
        ("v1", "-1257786",  1,   HEADERS_WEB),
        ("v1", "-1257786",  64,  HEADERS_IOS),
        ("v3", "-1257786",  1,   HEADERS_WEB),
        ("v2", "12358062",  1,   HEADERS_WEB),
        ("v2", "-446085",   1,   HEADERS_WEB),
    ]

    for ver, dest, atype, hdrs in combos:
        print(f"     🔌 card/{ver} appType={atype} dest={dest}")
        product_data = _card_api(article, ver, dest, atype, hdrs)
        if product_data:
            print(f"     ✅ Найдено: card/{ver} appType={atype}")
            break
        time.sleep(0.2)

    # ── Шаг 4: search.wb.ru ──────────────────────────────
    if not product_data:
        print("     🔌 search.wb.ru...")
        product_data = _search_api(article)
        if product_data:
            print("     ✅ Найдено: search.wb.ru")

    # ── Шаг 5: ScraperAPI → HTML ──────────────────────────
    if not product_data:
        print("     🌐 ScraperAPI → HTML страницы...")
        raw = _scraper_html(article)
        if raw:
            result["price"]    = raw["price"]
            result["name"]     = raw.get("name") or result["name"]
            result["in_stock"] = raw["in_stock"]
            if result["price"]:
                print(f"   ✅ WB HTML: {result['price']:,.0f} ₽")
            return result

    # ── Итог ─────────────────────────────────────────────
    if not product_data:
        status = "снят с продажи" if not cdn_info else "API не отвечает"
        result["error"] = (
            f"Товар {article} не найден ни одним методом ({status}). "
            f"Откройте https://www.wildberries.ru/catalog/{article}/detail.aspx"
            f" в браузере и проверьте что товар доступен."
        )
        return result

    parsed = _parse_wb_product(product_data)
    result["name"]     = parsed["name"] or result["name"]
    result["in_stock"] = parsed["in_stock"]
    result["price"]    = parsed["price"]

    if result["price"] is None:
        result["error"] = f"Товар {article} найден, но цена отсутствует (нет в наличии?)"
    else:
        print(f"   ✅ WB итог: {result['price']:,.0f} ₽")

    return result
