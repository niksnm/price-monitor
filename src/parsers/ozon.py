"""
parsers/ozon.py — Парсер Ozon через ScraperAPI
================================================

КАК РАБОТАЕТ:
  1. Запрос идёт НЕ напрямую на ozon.ru, а через ScraperAPI
  2. ScraperAPI запускает реальный браузер Chrome на своих серверах
  3. Страница полностью рендерится (JavaScript выполняется)
  4. Мы получаем готовый HTML как будто открыли в браузере
  5. Из HTML извлекаем цену несколькими методами

МЕТОДЫ ИЗВЛЕЧЕНИЯ ЦЕНЫ (в порядке приоритета):
  1. JSON-LD разметка (Schema.org) — самый надёжный
  2. Мета-теги с ценой
  3. JavaScript-объекты в тегах <script>
  4. CSS-селекторы по data-атрибутам
  5. Regex поиск по тексту страницы

ВАЖНО:
  Ozon периодически меняет структуру страниц — поэтому используем
  5 независимых методов. Если один перестаёт работать — остальные
  продолжают. Это делает парсер устойчивым к изменениям сайта.
"""

import re
import json
import sys
import os
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List

# Подключаем наш ScraperAPI клиент
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import scrape_url


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────

def _clean_price(text: str) -> Optional[float]:
    """
    Преобразует строку с ценой в число с плавающей точкой.

    ПРИМЕРЫ:
      "1 299 ₽"   → 1299.0
      "45 990,50"  → 45990.5
      "1299"       → 1299.0
      "abc"        → None
    """
    if not text:
        return None

    # Убираем всё кроме цифр, точки и запятой
    cleaned = re.sub(r'[^\d.,]', '', str(text))
    # Убираем пробелы и неразрывные пробелы
    cleaned = cleaned.replace('\u00a0', '').replace('\u202f', '').replace(' ', '')

    if not cleaned:
        return None

    # Заменяем запятую на точку (европейский формат)
    cleaned = cleaned.replace(',', '.')

    # Если несколько точек — убираем лишние (1.299.00 → 129900)
    parts = cleaned.split('.')
    if len(parts) > 2:
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]

    try:
        price = float(cleaned)
        # Проверяем разумность цены (от 1 рубля до 10 млн)
        if 1.0 <= price <= 10_000_000.0:
            return price
        return None
    except (ValueError, OverflowError):
        return None


def _is_valid_price(price: float) -> bool:
    """Проверяет что цена в разумных пределах."""
    return 1.0 <= price <= 10_000_000.0


# ─────────────────────────────────────────────────────────────
# МЕТОДЫ ИЗВЛЕЧЕНИЯ ЦЕНЫ
# ─────────────────────────────────────────────────────────────

def _extract_from_json_ld(soup: BeautifulSoup) -> Optional[float]:
    """
    МЕТОД 1: Извлечение из JSON-LD (Schema.org разметка)

    Ozon добавляет машиночитаемую разметку в формате JSON-LD.
    Пример разметки:
      <script type="application/ld+json">
        {"@type": "Product", "offers": {"price": "45990"}}
      </script>

    Это самый надёжный метод — эта разметка добавляется для
    поисковых систем и меняется реже чем визуальный дизайн.
    """
    scripts = soup.find_all("script", type="application/ld+json")

    for script in scripts:
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
            # Может быть список объектов или один объект
            items = data if isinstance(data, list) else [data]

            for item in items:
                # Ищем Product или Offer
                if item.get("@type") not in ("Product", "Offer"):
                    continue

                # Вариант 1: Product → offers → price
                offers = item.get("offers", {})
                if isinstance(offers, dict):
                    price_val = offers.get("price") or offers.get("lowPrice")
                    if price_val:
                        price = _clean_price(str(price_val))
                        if price:
                            return price

                # Вариант 2: Product → offers → список
                elif isinstance(offers, list) and offers:
                    for offer in offers:
                        price_val = offer.get("price") or offer.get("lowPrice")
                        if price_val:
                            price = _clean_price(str(price_val))
                            if price:
                                return price

                # Вариант 3: Прямо в объекте
                if item.get("@type") == "Offer":
                    price_val = item.get("price")
                    if price_val:
                        price = _clean_price(str(price_val))
                        if price:
                            return price

        except (json.JSONDecodeError, TypeError, AttributeError):
            continue

    return None


def _extract_from_meta(soup: BeautifulSoup) -> Optional[float]:
    """
    МЕТОД 2: Извлечение из мета-тегов

    Некоторые страницы содержат цену в мета-тегах:
      <meta property="product:price:amount" content="45990">
      <meta itemprop="price" content="45990">
    """
    price_metas = [
        ("property", "product:price:amount"),
        ("property", "og:price:amount"),
        ("itemprop", "price"),
        ("name", "price"),
    ]

    for attr, value in price_metas:
        tag = soup.find("meta", {attr: value})
        if tag:
            content = tag.get("content", "")
            price = _clean_price(content)
            if price:
                return price

    return None


def _extract_from_scripts(html: str) -> Optional[float]:
    """
    МЕТОД 3: Извлечение из JavaScript объектов

    Ozon хранит данные в JavaScript переменных внутри тегов <script>.
    Ищем паттерны вида:
      "price": 45990
      "finalPrice": 45990
      "salePrice": 45990
      "currentPrice": 45990
    """
    # Паттерны в порядке приоритета
    patterns = [
        # Ozon-специфичные ключи
        r'"finalPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"sellPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"cardPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"currentPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"discountedPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"salePrice"\s*:\s*(\d+(?:\.\d+)?)',
        # Общие паттерны
        r'"price"\s*:\s*(\d{3,7}(?:\.\d+)?)',  # Минимум 3 цифры (от 100 руб)
        r"'price'\s*:\s*(\d{3,7}(?:\.\d+)?)",
    ]

    # Кандидаты на цену
    candidates: List[float] = []

    for pattern in patterns:
        matches = re.findall(pattern, html)
        for match in matches:
            try:
                price = float(match)
                if _is_valid_price(price):
                    candidates.append(price)
            except ValueError:
                continue

    if not candidates:
        return None

    # Выбираем наиболее часто встречающуюся цену
    # (цена товара обычно повторяется на странице несколько раз)
    from collections import Counter
    counter = Counter(candidates)
    most_common_price, count = counter.most_common(1)[0]

    # Если цена встречается хотя бы 2 раза — это скорее всего она
    if count >= 2:
        return most_common_price

    # Иначе возвращаем первую найденную
    return candidates[0] if candidates else None


def _extract_from_css(soup: BeautifulSoup) -> Optional[float]:
    """
    МЕТОД 4: Извлечение по CSS-атрибутам

    Ищем элементы с атрибутом itemprop="price" или
    элементы с data-атрибутами содержащими цену.
    """
    # itemprop="price" — стандартный атрибут Schema.org
    el = soup.find(itemprop="price")
    if el:
        # Может быть в атрибуте content или в тексте
        content = el.get("content") or el.get_text(strip=True)
        price = _clean_price(content)
        if price:
            return price

    # data-price атрибут
    for tag in soup.find_all(attrs={"data-price": True}):
        price = _clean_price(tag["data-price"])
        if price:
            return price

    return None


def _extract_price_all_methods(html: str, soup: BeautifulSoup) -> Optional[float]:
    """
    Запускает все методы по очереди, возвращает первый успешный результат.
    """
    methods = [
        ("JSON-LD", lambda: _extract_from_json_ld(soup)),
        ("Meta-теги", lambda: _extract_from_meta(soup)),
        ("JavaScript", lambda: _extract_from_scripts(html)),
        ("CSS-атрибуты", lambda: _extract_from_css(soup)),
    ]

    for method_name, method_func in methods:
        try:
            price = method_func()
            if price:
                print(f"     💰 Цена найдена методом [{method_name}]: {price:,.0f} ₽")
                return price
        except Exception as e:
            print(f"     ⚠️  Метод [{method_name}] упал с ошибкой: {e}")
            continue

    return None


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ПАРСЕРЫ
# ─────────────────────────────────────────────────────────────

def _extract_name(soup: BeautifulSoup) -> str:
    """Извлекает название товара."""
    # Приоритет: h1 > og:title > title
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(strip=True)
        if len(name) > 5:
            return name[:300]

    og_title = soup.find("meta", property="og:title")
    if og_title:
        return og_title.get("content", "")[:300]

    title = soup.find("title")
    if title:
        # Убираем " — Ozon" из конца
        name = title.get_text(strip=True)
        name = re.sub(r'\s*[—\-|]\s*Ozon.*$', '', name, flags=re.IGNORECASE)
        return name[:300]

    return ""


def _check_in_stock(html: str, soup: BeautifulSoup) -> bool:
    """Проверяет наличие товара на складе."""
    # Признаки отсутствия
    out_of_stock_signals = [
        "нет в наличии",
        "товар недоступен",
        "нет на складе",
        "товар снят с продажи",
        "out of stock",
        "unavailable",
    ]
    html_lower = html.lower()
    for signal in out_of_stock_signals:
        if signal in html_lower:
            return False

    # Schema.org availability
    avail = soup.find(itemprop="availability")
    if avail:
        avail_text = (avail.get("content", "") + avail.get_text()).lower()
        if "instock" in avail_text or "in_stock" in avail_text:
            return True
        if "outofstock" in avail_text or "discontinued" in avail_text:
            return False

    # Если нет явных признаков отсутствия — считаем что есть
    return True


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара с Ozon через ScraperAPI.

    ВХОДНЫЕ ДАННЫЕ:
      url — полная ссылка на товар Ozon
            Пример: "https://www.ozon.ru/product/smartfon-apple-iphone-1234567/"

    ВОЗВРАЩАЕТ словарь:
      {
        "price":    float или None  — цена в рублях (None если не удалось)
        "name":     str             — название товара
        "in_stock": bool            — True если есть в наличии
        "error":    str или None    — описание ошибки (None если всё ок)
      }

    ПРИМЕРЫ РЕЗУЛЬТАТОВ:
      Успех:  {"price": 45990.0, "name": "Телефон ...", "in_stock": True, "error": None}
      Ошибка: {"price": None, "name": "", "in_stock": False, "error": "Причина"}
    """
    result = {
        "price": None,
        "name": "",
        "in_stock": False,
        "error": None,
        "source": "ozon"
    }

    print(f"   🔵 Ozon: получаем страницу через ScraperAPI...")

    # Получаем HTML через ScraperAPI
    # render_js=True обязательно — Ozon использует React/JavaScript
    html, error = scrape_url(
        url=url,
        render_js=True,           # Обязательно для Ozon
        country_code="ru",        # Российский IP обязателен
        retry_count=3,            # 3 попытки при ошибках
        retry_delay=8.0,          # 8 секунд между попытками
        timeout=70,               # До 70 секунд на JS-рендеринг
    )

    if error:
        result["error"] = f"ScraperAPI ошибка: {error}"
        return result

    if not html:
        result["error"] = "ScraperAPI вернул пустой ответ"
        return result

    # Парсим HTML
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        result["error"] = f"Ошибка парсинга HTML: {e}"
        return result

    # Извлекаем данные
    result["name"] = _extract_name(soup)
    result["in_stock"] = _check_in_stock(html, soup)
    result["price"] = _extract_price_all_methods(html, soup)

    if result["price"] is None:
        result["error"] = (
            "Цена не найдена ни одним методом. "
            "Возможные причины: Ozon изменил структуру страницы, "
            "или ScraperAPI не полностью отрендерил JavaScript. "
            "Попробуйте снова — при следующем запуске может сработать."
        )

    return result


# ─────────────────────────────────────────────────────────────
# ТЕСТ ПРИ ПРЯМОМ ЗАПУСКЕ
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "https://www.ozon.ru/product/smartfon-apple-iphone-15-pro-max-256-gb-chyornyy-titanium-1236462765/"
    )

    print(f"\n{'='*60}")
    print(f"🧪 Тест парсера Ozon")
    print(f"{'='*60}")
    print(f"URL: {test_url}\n")

    result = fetch_price(test_url)

    print(f"\n{'─'*40}")
    print(f"Результат:")
    print(f"  Цена:      {result['price']:,.0f} ₽" if result['price'] else "  Цена:      НЕ НАЙДЕНА")
    print(f"  Название:  {result['name'][:80]}" if result['name'] else "  Название:  НЕ НАЙДЕНО")
    print(f"  В наличии: {'✅ Да' if result['in_stock'] else '❌ Нет'}")
    if result['error']:
        print(f"  Ошибка:    {result['error']}")
    print(f"{'='*60}\n")
