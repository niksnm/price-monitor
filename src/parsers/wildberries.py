"""
parsers/wildberries.py — Парсер Wildberries v4.1
=================================================

ИСПРАВЛЕНИЕ БАГА "Товар не найден в WB API (ни v)":

  WB периодически меняет версии своего API.
  Старый код пробовал только v1 — который перестал работать для части товаров.

  РЕШЕНИЕ: перебираем несколько API-эндпоинтов по приоритету:
    1. card.wb.ru/cards/v2/detail  (текущий основной)
    2. card.wb.ru/cards/v1/detail  (старый, для совместимости)
    3. card.wb.ru/cards/v3/detail  (новый, для части товаров)

  Для каждой версии пробуем разные dest параметры:
    -1257786 = Москва (стандарт)
    -1073301 = Московская область
    -455203  = Санкт-Петербург

  ДОПОЛНИТЕЛЬНО — прямой запрос к search API:
    Если card API не нашёл товар — пробуем search.wb.ru
    который работает с артикулами напрямую.

КАК РАБОТАЕТ WB API:
  WB предоставляет бесплатный публичный API для карточек товаров.
  Цена хранится в КОПЕЙКАХ — делим на 100 чтобы получить рубли.
  salePriceU — цена со скидкой (та что видит пользователь)
  priceU     — цена без скидки (зачёркнутая)
"""

import re
import requests
import time
from typing import Optional, Dict, Any, List


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Origin": "https://www.wildberries.ru",
    "Referer": "https://www.wildberries.ru/",
}

# Параметры dest для разных регионов
DEST_VALUES = ["-1257786", "-1073301", "-455203"]

# Версии API в порядке приоритета
API_VERSIONS = ["v2", "v1", "v3"]


def extract_article(url: str) -> Optional[str]:
    """
    Извлекает числовой артикул из URL или строки.

    Поддерживаемые форматы:
      https://www.wildberries.ru/catalog/218412789/detail.aspx  → 218412789
      https://wildberries.ru/catalog/218412789/detail.aspx      → 218412789
      https://www.wildberries.ru/catalog/218412789              → 218412789
      218412789   (просто число)                                → 218412789
    """
    url = url.strip()

    # Просто число — это и есть артикул
    if url.isdigit():
        return url

    # Ищем /catalog/ЧИСЛО/ или /catalog/ЧИСЛО в конце
    match = re.search(r'/catalog/(\d+)(?:/|$)', url)
    if match:
        return match.group(1)

    # Последовательность цифр 6+ символов в URL
    match = re.search(r'\b(\d{6,})\b', url)
    if match:
        return match.group(1)

    return None


def _parse_product(product: dict) -> Dict[str, Any]:
    """
    Извлекает цену и другие данные из объекта товара WB API.

    WB хранит цены в копейках (умножены на 100).
    salePriceU — финальная цена (со скидкой WB + скидка продавца + СПП)
    priceU     — базовая цена без скидок
    """
    name = product.get("name", "")

    # Проверка наличия на складах
    sizes = product.get("sizes", [])
    in_stock = any(s.get("stocks") for s in sizes)

    # Цена: salePriceU = финальная цена покупателя (то что видно на сайте)
    sale_u  = product.get("salePriceU")
    price_u = product.get("priceU")

    price = None
    if sale_u and sale_u > 0:
        price = round(sale_u / 100, 2)
    elif price_u and price_u > 0:
        price = round(price_u / 100, 2)

    return {"price": price, "name": name, "in_stock": in_stock}


def _try_card_api(article: str, version: str, dest: str) -> Optional[Dict]:
    """
    Запрос к card.wb.ru — основной API карточек товаров.
    Возвращает словарь с данными товара или None при ошибке/не найдено.
    """
    url = (
        f"https://card.wb.ru/cards/{version}/detail"
        f"?appType=1&curr=rub&dest={dest}&nm={article}"
    )
    if version == "v1":
        url += "&spp=30"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()
        products = data.get("data", {}).get("products", [])
        if products:
            return products[0]
        return None
    except Exception:
        return None


def _try_search_api(article: str) -> Optional[Dict]:
    """
    Запрос к search.wb.ru — альтернативный API поиска.
    Работает даже когда card.wb.ru не возвращает товар.
    """
    url = (
        f"https://search.wb.ru/exactmatch/ru/common/v9/search"
        f"?query={article}&resultset=catalog&limit=1"
        f"&sort=popular&page=1&appType=1&curr=rub&dest=-1257786"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()
        products = data.get("data", {}).get("products", [])
        # Ищем точное совпадение по артикулу
        for p in products:
            if str(p.get("id", "")) == str(article):
                return p
        # Если точного нет — берём первый
        if products:
            return products[0]
        return None
    except Exception:
        return None


def _try_catalog_api(article: str) -> Optional[Dict]:
    """
    Запрос к catalog.wb.ru — ещё один запасной эндпоинт.
    """
    url = (
        f"https://catalog.wb.ru/cards/v1/detail"
        f"?appType=1&curr=rub&dest=-1257786&nm={article}"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()
        products = data.get("data", {}).get("products", [])
        return products[0] if products else None
    except Exception:
        return None


def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Wildberries.

    Стратегия (перебираем по порядку до первого успеха):
      1. card.wb.ru v2 + dest Москва
      2. card.wb.ru v1 + dest Москва
      3. card.wb.ru v3 + dest Москва
      4. card.wb.ru v2 + dest МО
      5. card.wb.ru v2 + dest СПб
      6. search.wb.ru  (поисковый API)
      7. catalog.wb.ru (запасной)

    Возвращает:
      {'price': float|None, 'name': str, 'in_stock': bool, 'error': str|None}
    """
    result = {"price": None, "name": "", "in_stock": False, "error": None}

    article = extract_article(url)
    if not article:
        result["error"] = f"Не удалось извлечь артикул из: {url[:80]}"
        return result

    print(f"   🍇 WB: артикул {article}")

    product_data = None
    tried = []

    # ── Уровень 1: card.wb.ru все версии + основной регион ──
    for version in API_VERSIONS:
        dest = DEST_VALUES[0]  # Москва
        key = f"{version}/{dest}"
        tried.append(key)
        print(f"     🔌 Пробуем card.wb.ru/{version} dest={dest}")
        product_data = _try_card_api(article, version, dest)
        if product_data:
            print(f"     ✅ Найдено через card.wb.ru/{version}")
            break
        time.sleep(0.5)  # Небольшая пауза между запросами

    # ── Уровень 2: другие регионы ────────────────────────────
    if not product_data:
        for dest in DEST_VALUES[1:]:
            key = f"v2/{dest}"
            tried.append(key)
            print(f"     🔌 Пробуем card.wb.ru/v2 dest={dest}")
            product_data = _try_card_api(article, "v2", dest)
            if product_data:
                print(f"     ✅ Найдено через card.wb.ru/v2 dest={dest}")
                break
            time.sleep(0.5)

    # ── Уровень 3: search.wb.ru ──────────────────────────────
    if not product_data:
        tried.append("search.wb.ru")
        print("     🔌 Пробуем search.wb.ru...")
        product_data = _try_search_api(article)
        if product_data:
            print("     ✅ Найдено через search.wb.ru")

    # ── Уровень 4: catalog.wb.ru ─────────────────────────────
    if not product_data:
        tried.append("catalog.wb.ru")
        print("     🔌 Пробуем catalog.wb.ru...")
        product_data = _try_catalog_api(article)
        if product_data:
            print("     ✅ Найдено через catalog.wb.ru")

    # ── Результат ────────────────────────────────────────────
    if not product_data:
        result["error"] = (
            f"Товар {article} не найден ни в одном WB API. "
            f"Проверьте что URL правильный и товар не снят с продажи. "
            f"Пробовали: {', '.join(tried)}"
        )
        return result

    # Извлекаем цену и данные
    parsed = _parse_product(product_data)
    result["name"]     = parsed["name"]
    result["in_stock"] = parsed["in_stock"]
    result["price"]    = parsed["price"]

    if result["price"] is None:
        result["error"] = (
            f"Товар {article} найден, но цена не определена. "
            f"Возможно товар недоступен в вашем регионе."
        )
    else:
        print(f"     ✅ WB итог: {result['price']:,.0f} ₽")

    return result


if __name__ == "__main__":
    import sys
    test = sys.argv[1] if len(sys.argv) > 1 else "218412789"
    print(f"\n{'='*55}\nТест WB парсера: {test}\n{'='*55}")
    r = fetch_price(test)
    print(f"\nЦена:     {r['price']:,.0f} ₽" if r['price'] else "\nЦена:     НЕ НАЙДЕНА")
    print(f"Название: {r['name'][:80]}")
    print(f"Наличие:  {'✅' if r['in_stock'] else '❌'}")
    if r['error']:
        print(f"Ошибка:   {r['error']}")
