"""
parsers/wildberries.py — Wildberries API парсер 2026
=====================================================

ИСПРАВЛЕНИЕ: Товар не найден ни в одном WB API
-----------------------------------------------
WB изменил структуру API в начале 2026.
Основные изменения:
  1. dest=-1257786 перестал работать для части товаров — нужны regions
  2. v1 API требует другие параметры чем раньше
  3. Добавлен новый параметр resultset=catalog

СТРАТЕГИЯ:
  Пробуем 5 разных форматов запроса по очереди.
  WB API публичный и не блокирует — ScraperAPI НЕ нужен.
  Если все API методы не дали результат — парсим страницу товара
  через ScraperAPI как последний шанс.
"""

import re
import json
import requests
import time
from typing import Optional, Dict, Any, List

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://www.wildberries.ru",
    "Referer": "https://www.wildberries.ru/",
    "Connection": "keep-alive",
}

# Все известные рабочие форматы dest для WB API 2025-2026
DEST_LIST = [
    "-1257786",    # Москва (основной)
    "-1059500",    # Другой московский склад
    "-2133462",    # Краснодар
    "-1123025",    # Новосибирск
    "12358062",    # Один из новых форматов
]


def extract_article(url: str) -> Optional[str]:
    """Извлекает артикул WB из URL или строки."""
    url = url.strip()
    if url.isdigit():
        return url
    # Стандартный формат /catalog/12345678/
    m = re.search(r'/catalog/(\d+)(?:/|$)', url)
    if m:
        return m.group(1)
    # Просто число в строке
    m = re.search(r'\b(\d{7,12})\b', url)
    if m:
        return m.group(1)
    return None


def _parse_product_data(product: dict) -> dict:
    """Извлекает цену, название и наличие из объекта товара."""
    name = product.get("name", "")

    # Наличие на складах
    sizes = product.get("sizes", [])
    in_stock = any(s.get("stocks") for s in sizes)

    # Цена в копейках → рубли
    # salePriceU = финальная цена покупателя
    # priceU     = базовая цена без скидок
    sale_u  = product.get("salePriceU", 0)
    price_u = product.get("priceU", 0)
    price   = None

    if sale_u and sale_u > 0:
        price = round(sale_u / 100, 2)
    elif price_u and price_u > 0:
        price = round(price_u / 100, 2)

    return {"price": price, "name": name, "in_stock": in_stock}


def _api_request(url: str, timeout: int = 10) -> Optional[dict]:
    """Выполняет GET запрос к WB API. Возвращает JSON или None."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None


def _try_card_api_v2(article: str) -> Optional[dict]:
    """
    card.wb.ru/cards/v2/detail — основной API 2025-2026.
    Пробуем разные dest.
    """
    for dest in DEST_LIST:
        url = (
            f"https://card.wb.ru/cards/v2/detail"
            f"?appType=1&curr=rub&dest={dest}&nm={article}"
        )
        data = _api_request(url)
        if data:
            products = data.get("data", {}).get("products", [])
            if products:
                return products[0]
        time.sleep(0.3)
    return None


def _try_card_api_v1(article: str) -> Optional[dict]:
    """card.wb.ru/cards/v1/detail — старый API."""
    for dest in ["-1257786", "-1059500"]:
        url = (
            f"https://card.wb.ru/cards/v1/detail"
            f"?appType=1&curr=rub&dest={dest}&spp=27&nm={article}"
        )
        data = _api_request(url)
        if data:
            products = data.get("data", {}).get("products", [])
            if products:
                return products[0]
        time.sleep(0.3)
    return None


def _try_card_api_v3(article: str) -> Optional[dict]:
    """card.wb.ru/cards/v3/detail — новый API."""
    url = (
        f"https://card.wb.ru/cards/v3/detail"
        f"?appType=1&curr=rub&dest=-1257786&nm={article}"
    )
    data = _api_request(url)
    if data:
        products = data.get("data", {}).get("products", [])
        if products:
            return products[0]
    return None


def _try_search_api(article: str) -> Optional[dict]:
    """
    search.wb.ru — поисковый API.
    Работает иначе чем card API, полезен как резерв.
    """
    for dest in ["-1257786", "-1059500"]:
        url = (
            f"https://search.wb.ru/exactmatch/ru/common/v9/search"
            f"?query={article}&resultset=catalog&limit=1"
            f"&sort=popular&page=1&appType=1&curr=rub&dest={dest}"
        )
        data = _api_request(url, timeout=12)
        if data:
            products = data.get("data", {}).get("products", [])
            # Ищем точное совпадение по id
            for p in products:
                if str(p.get("id", "")) == str(article):
                    return p
            if products:
                return products[0]
        time.sleep(0.3)
    return None


def _try_catalog_api(article: str) -> Optional[dict]:
    """catalog.wb.ru — дополнительный эндпоинт."""
    url = (
        f"https://catalog.wb.ru/cards/v1/detail"
        f"?appType=1&curr=rub&dest=-1257786&nm={article}"
    )
    data = _api_request(url)
    if data:
        products = data.get("data", {}).get("products", [])
        if products:
            return products[0]
    return None


def _try_product_page(article: str) -> Optional[dict]:
    """
    Прямой запрос на страницу товара WB через ScraperAPI.
    Последний шанс — парсим HTML страницы.
    """
    try:
        import sys
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from scraping_client import scrape_url

        url = f"https://www.wildberries.ru/catalog/{article}/detail.aspx"
        html, err = scrape_url(
            url=url,
            render_js=False,       # WB страница без JS даёт JSON данные
            country_code="ru",
            retry_count=2,
            retry_delay=5.0,
            timeout=30,
        )

        if err or not html:
            return None

        # WB вставляет данные товара в window.__wb_data или похожее
        # Ищем цену через regex
        patterns = [
            r'"salePriceU"\s*:\s*(\d+)',
            r'"priceU"\s*:\s*(\d+)',
            r'"finalPrice"\s*:\s*(\d+)',
            r'"cardPrice"\s*:\s*(\d+)',
        ]

        for pat in patterns:
            m = re.search(pat, html)
            if m:
                kopecks = int(m.group(1))
                if 100 <= kopecks <= 1_000_000_000:
                    price = round(kopecks / 100, 2)
                    # Ищем название
                    name_m = re.search(r'"name"\s*:\s*"([^"]{5,200})"', html)
                    name = name_m.group(1) if name_m else ""
                    return {
                        "price": price, "name": name,
                        "in_stock": True, "_from_html": True
                    }
    except Exception:
        pass
    return None


def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Wildberries.

    Пробует 6 методов по приоритету:
      1. card.wb.ru v2 (разные dest)
      2. card.wb.ru v1 (разные dest)
      3. card.wb.ru v3
      4. search.wb.ru
      5. catalog.wb.ru
      6. ScraperAPI → HTML страница товара
    """
    result = {"price": None, "name": "", "in_stock": False, "error": None}

    article = extract_article(url)
    if not article:
        result["error"] = f"Не удалось извлечь артикул из: {url[:80]}"
        return result

    print(f"   🍇 WB: артикул {article}")

    methods = [
        ("card/v2", lambda: _try_card_api_v2(article)),
        ("card/v1", lambda: _try_card_api_v1(article)),
        ("card/v3", lambda: _try_card_api_v3(article)),
        ("search",  lambda: _try_search_api(article)),
        ("catalog", lambda: _try_catalog_api(article)),
    ]

    product_data = None
    for name_method, fn in methods:
        print(f"     🔌 Пробуем {name_method}...")
        try:
            product_data = fn()
        except Exception as e:
            print(f"     ⚠️  {name_method} упал: {e}")
            product_data = None

        if product_data:
            print(f"     ✅ Найдено через {name_method}")
            break

    # Последний шанс — HTML страница через ScraperAPI
    if not product_data:
        print("     🌐 Пробуем HTML страницу через ScraperAPI...")
        try:
            raw = _try_product_page(article)
            if raw:
                # Это уже готовый результат
                result["price"]    = raw.get("price")
                result["name"]     = raw.get("name", "")
                result["in_stock"] = raw.get("in_stock", True)
                if result["price"]:
                    print(f"     ✅ WB HTML → {result['price']:,.0f} ₽")
                return result
        except Exception:
            pass

    if not product_data:
        result["error"] = (
            f"Товар {article} не найден ни одним методом. "
            f"Проверьте что ссылка правильная и товар не снят с продажи. "
            f"Откройте wildberries.ru/catalog/{article}/detail.aspx в браузере."
        )
        return result

    # Если данные получены как словарь с уже готовой ценой
    if "_from_html" in product_data:
        result["price"]    = product_data["price"]
        result["name"]     = product_data["name"]
        result["in_stock"] = product_data["in_stock"]
    else:
        parsed = _parse_product_data(product_data)
        result["name"]     = parsed["name"]
        result["in_stock"] = parsed["in_stock"]
        result["price"]    = parsed["price"]

    if result["price"] is None:
        result["error"] = (
            f"Товар {article} найден, но цена не определена. "
            f"Возможно товар недоступен для заказа."
        )
    else:
        print(f"   ✅ WB итог: {result['price']:,.0f} ₽")

    return result


if __name__ == "__main__":
    import sys
    test = sys.argv[1] if len(sys.argv) > 1 else "218412789"
    print(f"\n{'='*55}\nТест WB: {test}\n{'='*55}")
    r = fetch_price(test)
    print(f"\nЦена:     {r['price']:,.0f} ₽" if r['price'] else "\nЦена:     НЕ НАЙДЕНА")
    print(f"Название: {r['name'][:80]}")
    print(f"Наличие:  {'✅' if r['in_stock'] else '❌'}")
    if r['error']:
        print(f"Ошибка:   {r['error']}")
