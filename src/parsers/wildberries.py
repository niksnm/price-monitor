"""
parsers/wildberries.py — парсер Wildberries через официальное API
WB предоставляет бесплатное публичное API для получения цен — самый надёжный парсер.
"""

import re
import requests
from typing import Optional, Dict, Any


def extract_article(url: str) -> Optional[str]:
    """
    Извлекает артикул из URL Wildberries.
    Поддерживаемые форматы:
      https://www.wildberries.ru/catalog/12345678/detail.aspx
      https://wildberries.ru/catalog/12345678/detail.aspx
      Просто артикул: '12345678'
    """
    # Если передан просто числовой артикул
    if url.strip().isdigit():
        return url.strip()

    # Извлечение из URL
    match = re.search(r'/catalog/(\d+)/', url)
    if match:
        return match.group(1)

    return None


def get_basket_host(vol: int) -> str:
    """Определяет CDN-хост по объёму артикула (логика WB)."""
    if vol <= 143:   return "01"
    if vol <= 287:   return "02"
    if vol <= 431:   return "03"
    if vol <= 719:   return "04"
    if vol <= 1007:  return "05"
    if vol <= 1061:  return "06"
    if vol <= 1115:  return "07"
    if vol <= 1169:  return "08"
    if vol <= 1313:  return "09"
    if vol <= 1601:  return "10"
    if vol <= 1655:  return "11"
    if vol <= 1919:  return "12"
    if vol <= 2045:  return "13"
    if vol <= 2189:  return "14"
    if vol <= 2405:  return "15"
    if vol <= 2621:  return "16"
    if vol <= 2837:  return "17"
    return "18"


def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Wildberries через официальное Card API.

    Returns:
        {
          'price': float или None,
          'name': str,
          'in_stock': bool,
          'error': str или None,
          'raw': dict  # полный ответ API
        }
    """
    result = {"price": None, "name": "", "in_stock": False,
              "error": None, "raw": {}}

    article = extract_article(url)
    if not article:
        result["error"] = f"Не удалось извлечь артикул из URL: {url}"
        return result

    try:
        # WB Card API — официальный эндпоинт
        api_url = (
            f"https://card.wb.ru/cards/v1/detail"
            f"?appType=1&curr=rub&dest=-1257786"
            f"&spp=30&nm={article}"
        )

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Origin": "https://www.wildberries.ru",
            "Referer": "https://www.wildberries.ru/"
        }

        resp = requests.get(api_url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        products = data.get("data", {}).get("products", [])
        if not products:
            result["error"] = f"Товар {article} не найден в WB API"
            return result

        product = products[0]
        result["raw"] = product
        result["name"] = product.get("name", "")

        # Проверка наличия на складе
        sizes = product.get("sizes", [])
        has_stock = any(
            s.get("stocks") for s in sizes
        )
        result["in_stock"] = has_stock

        # Цена: WB хранит в копейках (делим на 100)
        # salePriceU — цена со скидкой, priceU — без скидки
        sale_price = product.get("salePriceU")
        basic_price = product.get("priceU")

        if sale_price:
            result["price"] = round(sale_price / 100, 2)
        elif basic_price:
            result["price"] = round(basic_price / 100, 2)
        else:
            result["error"] = "Цена не найдена в ответе API"

        return result

    except requests.exceptions.Timeout:
        result["error"] = "Таймаут запроса к WB API"
    except requests.exceptions.HTTPError as e:
        result["error"] = f"HTTP ошибка WB API: {e}"
    except requests.exceptions.RequestException as e:
        result["error"] = f"Ошибка сети WB: {e}"
    except (KeyError, ValueError, TypeError) as e:
        result["error"] = f"Ошибка парсинга ответа WB: {e}"

    return result


if __name__ == "__main__":
    # Тест парсера
    test_url = "https://www.wildberries.ru/catalog/15449797/detail.aspx"
    print(f"Тестируем: {test_url}")
    res = fetch_price(test_url)
    print(f"Цена: {res['price']} ₽")
    print(f"Название: {res['name']}")
    print(f"В наличии: {res['in_stock']}")
    if res["error"]:
        print(f"Ошибка: {res['error']}")
