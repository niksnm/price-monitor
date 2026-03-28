"""
parsers/wildberries.py  v5  — WB через официальное API
=======================================================

ПОЧЕМУ v4 НЕ РАБОТАЛ:
  1. Endpoint card.wb.ru/cards/v1/detail устарел.
     WB переключился на v2. v1 до сих пор работает,
     но иногда возвращает пустой products[] или старые данные.

  2. Поле salePriceU: WB ввёл новую систему цен.
     Теперь финальная цена может лежать в нескольких местах:
       - product["salePriceU"]          (старый формат, в копейках)
       - product["extended"]["clientPriceU"]  (цена для клуба WB)
       - sizes[N]["price"]["total"]      (новый формат v2, в копейках)
       - sizes[N]["price"]["product"]   (цена без дополнительных скидок)

  3. dest=-1257786 — иногда WB меняет логику dest.
     Добавлен fallback на другие популярные регионы.

СТРАТЕГИЯ v5:
  1. Пробуем v2 endpoint с правильными dest
  2. Извлекаем цену из нескольких полей (приоритет: клиентская → продажная → базовая)
  3. При ошибке — пробуем v1 как fallback
  4. Проверка наличия по sizes[N].stocks
"""

import re
import requests
from typing import Optional, Dict, Any, List


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────

def extract_article(url: str) -> Optional[str]:
    """
    Извлекает артикул из URL Wildberries.
    Поддерживает форматы:
      https://www.wildberries.ru/catalog/12345678/detail.aspx
      https://wildberries.ru/catalog/12345678/detail.aspx
      Просто числовой артикул: '12345678'
    """
    url = url.strip()
    if url.isdigit():
        return url

    m = re.search(r'/catalog/(\d+)/', url)
    if m:
        return m.group(1)

    # Попробуем найти длинное число в URL (артикул обычно 7-9 цифр)
    m = re.search(r'(\d{7,10})', url)
    if m:
        return m.group(1)

    return None


def _price_from_kopecks(val) -> Optional[float]:
    """Конвертирует копейки в рубли. WB хранит цены ×100."""
    try:
        kopecks = int(val)
        if kopecks <= 0:
            return None
        rubles = kopecks / 100
        return rubles if 10.0 <= rubles <= 10_000_000.0 else None
    except (TypeError, ValueError):
        return None


def _extract_price_from_product(product: dict) -> Optional[float]:
    """
    Извлекает финальную цену из объекта product.
    Пробует поля в порядке приоритета.

    Порядок (от наиболее точного к наименее):
      1. sizes[N].price.total        (v2 новый формат, итого к оплате)
      2. extended.clientPriceU       (цена для клуба WB, в копейках)
      3. salePriceU                  (цена со скидкой, в копейках)
      4. extended.basicSalePrice     (иногда прямо в рублях)
      5. priceU                      (базовая цена без скидки, в копейках)
    """
    # 1. Sizes → price.total (v2 формат)
    sizes = product.get('sizes', [])
    for size in sizes:
        price_block = size.get('price') or {}
        if isinstance(price_block, dict):
            # В v2: total, product, basic — все в копейках
            for field in ('total', 'product', 'basic'):
                p = _price_from_kopecks(price_block.get(field))
                if p:
                    return p

    # 2. extended.clientPriceU (цена для участников клуба WB)
    extended = product.get('extended') or {}
    p = _price_from_kopecks(extended.get('clientPriceU'))
    if p:
        return p

    # 3. salePriceU — цена со скидкой (старый формат, в копейках)
    p = _price_from_kopecks(product.get('salePriceU'))
    if p:
        return p

    # 4. extended.basicSalePrice — иногда хранится прямо в рублях
    bsp = extended.get('basicSalePrice')
    if bsp:
        try:
            f = float(bsp)
            if 10.0 <= f <= 10_000_000.0:
                return f
        except (TypeError, ValueError):
            pass

    # 5. priceU — исходная цена без скидки (в копейках)
    p = _price_from_kopecks(product.get('priceU'))
    if p:
        return p

    return None


def _check_in_stock(product: dict) -> bool:
    """
    Проверяет наличие товара.
    В WB API: sizes[N].stocks — список объектов складов.
    Пустой список = нет в наличии.
    """
    sizes = product.get('sizes', [])
    for size in sizes:
        stocks = size.get('stocks', [])
        if isinstance(stocks, list) and len(stocks) > 0:
            # Дополнительно проверяем qty если поле есть
            for stock in stocks:
                if isinstance(stock, dict):
                    qty = stock.get('qty', 1)
                    if qty and qty > 0:
                        return True
                else:
                    return True  # Есть запись = есть товар
    return False


# ─────────────────────────────────────────────────────────────
# ЗАПРОС К WB API
# ─────────────────────────────────────────────────────────────

_HEADERS = {
    'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/124.0.0.0 Safari/537.36',
    'Accept':          'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9',
    'Origin':          'https://www.wildberries.ru',
    'Referer':         'https://www.wildberries.ru/',
}

# Регионы/корзины WB (dest). -1257786 = Москва (основной).
# Если не работает — пробуем Россия в целом.
_DEST_OPTIONS = ['-1257786', '-1']


def _fetch_api(article: str, version: str = 'v2') -> Optional[dict]:
    """
    Делает запрос к WB Cards API и возвращает данные продукта или None.

    version: 'v2' (основной) или 'v1' (fallback)
    """
    for dest in _DEST_OPTIONS:
        url = (
            f'https://card.wb.ru/cards/{version}/detail'
            f'?appType=1&curr=rub&dest={dest}&nm={article}'
        )
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            products = data.get('data', {}).get('products', [])
            if products:
                return products[0]

        except requests.exceptions.Timeout:
            print(f'     ⏱️  Таймаут WB API ({version}, dest={dest})')
        except requests.exceptions.HTTPError as e:
            print(f'     ❌ HTTP ошибка WB API: {e}')
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f'     ❌ Ошибка WB API: {e}')

    return None


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара Wildberries через официальное Cards API.

    Стратегия:
      1. Извлекаем артикул из URL
      2. Запрашиваем v2 API (новый, более полные данные)
      3. Если v2 не отдал продукт — пробуем v1 (старый)
      4. Извлекаем цену из нескольких возможных полей

    Возвращает:
      {"price": float|None, "name": str, "in_stock": bool,
       "error": str|None, "source": "wildberries"}
    """
    result = {
        'price':    None,
        'name':     '',
        'in_stock': False,
        'error':    None,
        'source':   'wildberries',
    }

    # Шаг 1: Артикул
    article = extract_article(url)
    if not article:
        result['error'] = f'Не удалось извлечь артикул из URL: {url}'
        return result

    print(f'   🍇 Wildberries: артикул {article}')

    # Шаг 2: Запрос v2
    product = _fetch_api(article, version='v2')

    # Шаг 3: Fallback на v1
    if product is None:
        print('     ↩️  v2 не дал данных, пробуем v1...')
        product = _fetch_api(article, version='v1')

    if product is None:
        result['error'] = (
            f'Товар {article} не найден в WB API (ни v1 ни v2). '
            'Проверьте артикул или URL.'
        )
        return result

    # Шаг 4: Извлекаем данные
    result['name']     = product.get('name', '')
    result['in_stock'] = _check_in_stock(product)
    result['price']    = _extract_price_from_product(product)

    if result['price'] is None:
        # Дополнительная диагностика
        has_sizes = bool(product.get('sizes'))
        result['error'] = (
            f'Цена WB не найдена (артикул {article}). '
            f'Размеры в ответе: {"есть" if has_sizes else "нет"}. '
            'Возможно товар снят с продажи или API вернул неполные данные.'
        )
    else:
        print(f'   ✅ Wildberries: {result["price"]:,.0f} ₽')

    return result


if __name__ == '__main__':
    import sys
    test_url = (sys.argv[1] if len(sys.argv) > 1
                else 'https://www.wildberries.ru/catalog/15449797/detail.aspx')
    print(f'\n{"="*60}\nТест WB v5\n{"="*60}')
    r = fetch_price(test_url)
    print(f'\nЦена:     {r["price"]:,.0f} ₽' if r['price'] else '\nЦена:     НЕ НАЙДЕНА')
    print(f'Название: {r["name"][:80]}')
    print(f'Наличие:  {"✅" if r["in_stock"] else "❌"}')
    if r['error']:
        print(f'Ошибка:   {r["error"]}')
