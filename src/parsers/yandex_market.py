"""
parsers/yandex_market.py — Парсер Яндекс.Маркет через ScraperAPI
=================================================================

КАК РАБОТАЕТ:
  Яндекс.Маркет использует серьёзную антибот защиту:
  - Яндекс SmartCaptcha (трудно обойти)
  - Fingerprinting браузера
  - Анализ поведения пользователя
  - Блокировка по IP диапазонам

  ScraperAPI обходит всё это используя:
  - Реальные резидентные IP из России
  - Полный браузерный fingerprint
  - Автоматическое решение капч

МЕТОДЫ ИЗВЛЕЧЕНИЯ ЦЕНЫ:
  1. JSON-LD разметка (если Яндекс добавил — редко)
  2. JavaScript данные в __NEXT_DATA__ (Next.js приложение)
  3. JavaScript переменные в скриптах
  4. CSS-атрибуты и data-атрибуты
  5. Regex паттерны по тексту
"""

import re
import json
import sys
import os
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import scrape_url


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────

def _clean_price(text: str) -> Optional[float]:
    """Извлекает числовое значение цены из строки."""
    if not text:
        return None
    cleaned = re.sub(r'[^\d.,]', '', str(text))
    cleaned = cleaned.replace('\u00a0', '').replace('\u202f', '').replace(' ', '')
    if not cleaned:
        return None
    cleaned = cleaned.replace(',', '.')
    parts = cleaned.split('.')
    if len(parts) > 2:
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1]
    try:
        price = float(cleaned)
        return price if 1.0 <= price <= 10_000_000.0 else None
    except (ValueError, OverflowError):
        return None


# ─────────────────────────────────────────────────────────────
# МЕТОДЫ ИЗВЛЕЧЕНИЯ ЦЕНЫ
# ─────────────────────────────────────────────────────────────

def _extract_from_next_data(html: str) -> Optional[float]:
    """
    МЕТОД 1: Данные из __NEXT_DATA__

    Яндекс.Маркет — это React/Next.js приложение.
    Next.js помещает начальные данные страницы в специальный тег:
      <script id="__NEXT_DATA__" type="application/json">
        {...огромный JSON с данными страницы...}
      </script>

    В этом JSON есть полные данные о товаре включая цену.
    Это самый надёжный метод для ЯМ.
    """
    # Ищем __NEXT_DATA__ в HTML
    pattern = r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>'
    match = re.search(pattern, html, re.DOTALL)

    if not match:
        return None

    try:
        data = json.loads(match.group(1))

        # Рекурсивно ищем ключи связанные с ценой
        price_keys = {
            'price', 'priceValue', 'currentPrice', 'minPrice',
            'value', 'amount', 'priceWithDiscount', 'discountedPrice',
            'basePrice', 'offerPrice', 'salePrice'
        }

        candidates = _find_prices_in_json(data, price_keys)

        if candidates:
            # Берём медианную цену чтобы отфильтровать выбросы
            candidates.sort()
            median_idx = len(candidates) // 2
            return candidates[median_idx]

    except (json.JSONDecodeError, TypeError):
        pass

    return None


def _find_prices_in_json(obj, price_keys: set, depth: int = 0) -> List[float]:
    """
    Рекурсивно обходит JSON и собирает все значения с ключами-ценами.

    Ограничение глубины depth=15 защищает от бесконечной рекурсии
    на очень вложенных объектах.
    """
    if depth > 15:
        return []

    results = []

    if isinstance(obj, dict):
        for key, value in obj.items():
            # Если ключ похож на "цена"
            if key.lower() in price_keys or any(pk in key.lower() for pk in price_keys):
                if isinstance(value, (int, float)):
                    if 1 <= value <= 10_000_000:
                        results.append(float(value))
                elif isinstance(value, str):
                    price = _clean_price(value)
                    if price:
                        results.append(price)
            # Рекурсия в значения
            results.extend(_find_prices_in_json(value, price_keys, depth + 1))

    elif isinstance(obj, list):
        for item in obj[:50]:  # Не больше 50 элементов списка
            results.extend(_find_prices_in_json(item, price_keys, depth + 1))

    return results


def _extract_from_json_ld(soup: BeautifulSoup) -> Optional[float]:
    """МЕТОД 2: JSON-LD разметка (Schema.org)"""
    for script in soup.find_all("script", type="application/ld+json"):
        if not script.string:
            continue
        try:
            data = json.loads(script.string)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("Product", "Offer"):
                    continue
                offers = item.get("offers", {})
                if isinstance(offers, dict):
                    p = offers.get("price") or offers.get("lowPrice")
                    if p:
                        price = _clean_price(str(p))
                        if price:
                            return price
                elif isinstance(offers, list) and offers:
                    p = offers[0].get("price")
                    if p:
                        price = _clean_price(str(p))
                        if price:
                            return price
        except Exception:
            continue
    return None


def _extract_from_scripts(html: str) -> Optional[float]:
    """МЕТОД 3: Паттерны в JavaScript коде"""
    patterns = [
        # Яндекс.Маркет специфичные
        r'"priceValue"\s*:\s*(\d+(?:\.\d+)?)',
        r'"currentPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"price"\s*:\s*\{\s*"value"\s*:\s*(\d+(?:\.\d+)?)',
        r'"minPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"offerPrice"\s*:\s*(\d+(?:\.\d+)?)',
        r'"priceWithDiscount"\s*:\s*(\d+(?:\.\d+)?)',
        # Общие
        r'"price"\s*:\s*(\d{3,7}(?:\.\d+)?)',
        r"'price'\s*:\s*(\d{3,7}(?:\.\d+)?)",
        r'price["\']?\s*:\s*["\']?(\d{3,7})',
    ]

    candidates = []
    for pattern in patterns:
        matches = re.findall(pattern, html)
        for match in matches:
            try:
                price = float(match)
                if 1 <= price <= 10_000_000:
                    candidates.append(price)
            except ValueError:
                continue

    if not candidates:
        return None

    # Наиболее частое значение
    from collections import Counter
    counter = Counter(candidates)
    price, count = counter.most_common(1)[0]
    return price if count >= 1 else None


def _extract_from_meta(soup: BeautifulSoup) -> Optional[float]:
    """МЕТОД 4: Мета-теги"""
    for attr, value in [
        ("property", "product:price:amount"),
        ("property", "og:price:amount"),
        ("itemprop", "price"),
        ("name", "price"),
    ]:
        tag = soup.find("meta", {attr: value})
        if tag:
            price = _clean_price(tag.get("content", ""))
            if price:
                return price
    return None


def _extract_name(soup: BeautifulSoup, html: str) -> str:
    """Извлекает название товара."""
    h1 = soup.find("h1")
    if h1:
        name = h1.get_text(strip=True)
        if len(name) > 5:
            return name[:300]

    # Из og:title убираем " — Яндекс Маркет"
    og = soup.find("meta", property="og:title")
    if og:
        name = og.get("content", "")
        name = re.sub(r'\s*[—\-|]\s*(Яндекс\.?Маркет|Маркет).*$', '', name, flags=re.IGNORECASE)
        if name:
            return name[:300]

    title = soup.find("title")
    if title:
        name = title.get_text(strip=True)
        name = re.sub(r'\s*[—\-|]\s*(Яндекс\.?Маркет|Маркет).*$', '', name, flags=re.IGNORECASE)
        return name[:300]

    return ""


def _check_in_stock(html: str, soup: BeautifulSoup) -> bool:
    """Проверяет наличие."""
    out_signals = [
        "нет в наличии", "нет на складе", "товар снят",
        "закончился", "недоступен", "out of stock", "unavailable"
    ]
    html_lower = html.lower()
    for signal in out_signals:
        if signal in html_lower:
            return False

    avail = soup.find(itemprop="availability")
    if avail:
        a = (avail.get("content", "") + avail.get_text()).lower()
        if "instock" in a:
            return True
        if "outofstock" in a:
            return False

    return True


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает цену товара с Яндекс.Маркет через ScraperAPI.

    ВХОДНЫЕ ДАННЫЕ:
      url — полная ссылка на товар ЯМ
            Пример: "https://market.yandex.ru/product--iphone-15/123456789"

    ВОЗВРАЩАЕТ:
      {"price": float, "name": str, "in_stock": bool, "error": str|None}
    """
    result = {
        "price": None,
        "name": "",
        "in_stock": False,
        "error": None,
        "source": "yandex_market"
    }

    print(f"   🟡 Яндекс.Маркет: получаем страницу через ScraperAPI...")

    html, error = scrape_url(
        url=url,
        render_js=True,
        country_code="ru",
        retry_count=3,
        retry_delay=10.0,  # ЯМ иногда медленнее отвечает
        timeout=90,        # До 90 секунд — ЯМ грузится дольше
        ultra_premium=True,  # ЯМ требует premium прокси
    )

    if error:
        result["error"] = f"ScraperAPI ошибка: {error}"
        return result

    if not html:
        result["error"] = "ScraperAPI вернул пустой ответ"
        return result

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        result["error"] = f"Ошибка парсинга HTML: {e}"
        return result

    result["name"] = _extract_name(soup, html)
    result["in_stock"] = _check_in_stock(html, soup)

    # Пробуем все методы по очереди
    methods = [
        ("__NEXT_DATA__", lambda: _extract_from_next_data(html)),
        ("JSON-LD", lambda: _extract_from_json_ld(soup)),
        ("JavaScript", lambda: _extract_from_scripts(html)),
        ("Meta-теги", lambda: _extract_from_meta(soup)),
    ]

    for method_name, method_func in methods:
        try:
            price = method_func()
            if price:
                print(f"     💰 Цена найдена методом [{method_name}]: {price:,.0f} ₽")
                result["price"] = price
                break
        except Exception as e:
            print(f"     ⚠️  Метод [{method_name}] ошибка: {e}")
            continue

    if result["price"] is None:
        result["error"] = (
            "Цена не найдена. "
            "Яндекс.Маркет использует очень сильную защиту — "
            "попробуйте запустить снова или используйте SCRAPER_PREMIUM=true."
        )

    return result


if __name__ == "__main__":
    import sys
    test_url = (
        sys.argv[1] if len(sys.argv) > 1
        else "https://market.yandex.ru/product--smartfon-apple-iphone-15/1837744073"
    )
    print(f"\n{'='*60}")
    print(f"🧪 Тест парсера Яндекс.Маркет")
    print(f"{'='*60}")
    result = fetch_price(test_url)
    print(f"\nРезультат:")
    print(f"  Цена:     {result['price']:,.0f} ₽" if result['price'] else "  Цена:     НЕ НАЙДЕНА")
    print(f"  Название: {result['name'][:80]}")
    print(f"  Наличие:  {'✅' if result['in_stock'] else '❌'}")
    if result['error']:
        print(f"  Ошибка:   {result['error']}")
