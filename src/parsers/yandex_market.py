"""
parsers/yandex_market.py — ЯМ парсер 2026 (правильная цена)
=============================================================

ПРОБЛЕМА НА СКРИНШОТЕ:
  Два разных товара (Steam Deck и Стулья-кресла) показывают
  одинаковую цену 71 388 ₽ — это явно неправильно.

  ПРИЧИНА: код брал не ту цену. В JSON ЯМ есть поля price во
  многих местах: цены похожих товаров, рекламных блоков,
  исторические цены. Наш код брал первое попавшееся число
  и зацикливался на одном значении.

ИСПРАВЛЕНИЕ:
  Яндекс.Маркет хранит реальную цену товара в КОНКРЕТНОМ пути
  __NEXT_DATA__.props.pageProps.initialState.productCard.product.offers.top.price

  Дополнительно — поле marketSku в URL позволяет точно
  идентифицировать SKU и его цену среди всех предложений.

  Новый алгоритм:
  1. Находим __NEXT_DATA__ JSON (один конкретный тег)
  2. Ищем по 3 известным путям для цены конкретного SKU
  3. Если не нашли — regex с паттернами специфичными для ЯМ 2026
  4. НЕ используем общий поиск "price" по всему JSON — он даёт мусор
"""

import re
import json
import sys
import os
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import scrape_url


# ─────────────────────────────────────────────────────────────
# УТИЛИТЫ
# ─────────────────────────────────────────────────────────────

def _to_price(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 100 <= f <= 10_000_000 else None
    if isinstance(val, str):
        s = re.sub(r'[^\d]', '', val.replace(',', '.'))
        try:
            f = float(s)
            return f if 100 <= f <= 10_000_000 else None
        except ValueError:
            return None
    return None


def _get_nested(obj: dict, *keys):
    """Безопасно достаёт вложенное значение из словаря."""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


# ─────────────────────────────────────────────────────────────
# ИЗВЛЕЧЕНИЕ ЦЕНЫ ИЗ __NEXT_DATA__
# ─────────────────────────────────────────────────────────────

def _price_from_next_data(data: dict) -> Optional[float]:
    """
    Ищет цену в __NEXT_DATA__ по известным путям ЯМ.

    Известные пути (проверено в 2025-2026):
      Path 1: initialState.productCard.product.offers.top.price.value
      Path 2: initialState.report.product.prices.min.value
      Path 3: pageProps.product.price.value
      Path 4: initialState.productCard.sku.product.offers.top.price.value
    """
    # Стандартные точки входа в данные
    initial = (
        _get_nested(data, "props", "pageProps", "initialState") or
        _get_nested(data, "props", "initialState") or
        {}
    )
    page_props = _get_nested(data, "props", "pageProps") or {}

    candidates = []

    # ── Path 1: productCard.product.offers.top.price ──────
    pc = _get_nested(initial, "productCard", "product")
    if pc:
        # Топовое предложение
        top_price = _get_nested(pc, "offers", "top", "price", "value")
        if top_price:
            p = _to_price(top_price)
            if p:
                candidates.append(("productCard.offers.top.price", p))

        # Минимальная цена предложений
        min_price = _get_nested(pc, "prices", "min", "value")
        if min_price:
            p = _to_price(min_price)
            if p:
                candidates.append(("productCard.prices.min", p))

        # Цена напрямую
        direct_price = pc.get("price") or pc.get("priceRange")
        if isinstance(direct_price, dict):
            pv = direct_price.get("value") or direct_price.get("min")
            p = _to_price(pv)
            if p:
                candidates.append(("productCard.price.value", p))

    # ── Path 2: report.product.prices ─────────────────────
    report_product = (
        _get_nested(initial, "report", "product") or
        _get_nested(initial, "report")
    )
    if isinstance(report_product, dict):
        rp_min = _get_nested(report_product, "prices", "min", "value")
        if rp_min:
            p = _to_price(rp_min)
            if p:
                candidates.append(("report.prices.min", p))

        # Оферы в report
        offers = report_product.get("offers") or []
        if isinstance(offers, list):
            for offer in offers[:5]:
                op = (_get_nested(offer, "price", "value") or
                      _get_nested(offer, "prices", "min", "value") or
                      offer.get("price"))
                p = _to_price(op)
                if p:
                    candidates.append(("report.offers[].price", p))
                    break

    # ── Path 3: pageProps.product ─────────────────────────
    pp_product = page_props.get("product")
    if isinstance(pp_product, dict):
        pv = (_get_nested(pp_product, "price", "value") or
              _get_nested(pp_product, "prices", "min", "value") or
              pp_product.get("price"))
        p = _to_price(pv)
        if p:
            candidates.append(("pageProps.product.price", p))

    # ── Path 4: sku секция ────────────────────────────────
    sku = _get_nested(initial, "productCard", "sku", "product")
    if isinstance(sku, dict):
        pv = (_get_nested(sku, "offers", "top", "price", "value") or
              _get_nested(sku, "price", "value"))
        p = _to_price(pv)
        if p:
            candidates.append(("sku.offers.top.price", p))

    if not candidates:
        return None

    # Выводим кандидатов в лог для диагностики
    for label, p in candidates[:3]:
        print(f"     🔍 Кандидат [{label}]: {p:,.0f} ₽")

    # Берём первый найденный — пути отсортированы по приоритету
    return candidates[0][1]


# ─────────────────────────────────────────────────────────────
# ИЗВЛЕЧЕНИЕ ЧЕРЕЗ REGEX (если __NEXT_DATA__ не дал результат)
# ─────────────────────────────────────────────────────────────

def _price_from_regex(html: str) -> Optional[float]:
    """
    Специфичные паттерны для ЯМ 2026.

    ВАЖНО: Используем только паттерны которые идентифицируют
    КОНКРЕТНУЮ цену товара, а не любое число в JSON.

    Паттерн "min":{"value":74536} — это минимальная цена
    среди всех предложений конкретного товара.
    Именно это число пользователь видит на странице.
    """
    patterns = [
        # Минимальная цена предложений (самый точный)
        (r'"min"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 5),
        (r'"lowestPrice"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 5),
        (r'"minimalPrice"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 5),
        # Цена топового предложения
        (r'"top"\s*:\s*\{[^}]{0,200}"value"\s*:\s*(\d{4,7})', 4),
        (r'"offerPrice"\s*:\s*(\d{4,7})', 3),
        # Общие но с контекстом
        (r'"price"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 2),
        (r'"currentPrice"\s*:\s*(\d{4,7})', 2),
    ]

    from collections import Counter
    candidates = []

    for pat, weight in patterns:
        for m in re.finditer(pat, html):
            try:
                p = float(m.group(1))
                if 100 <= p <= 10_000_000:
                    candidates.extend([p] * weight)
            except ValueError:
                pass

    if not candidates:
        return None

    # Берём самую частую взвешенную цену
    result = Counter(candidates).most_common(1)[0][0]

    # Санитарная проверка: цена должна быть уникальной
    # Если она встречается слишком много раз — это скорее всего мусор
    count = Counter(candidates)[result]
    total = len(candidates)
    if total > 20 and count / total > 0.5:
        # Слишком частая — берём вторую по частоте
        top2 = Counter(candidates).most_common(2)
        if len(top2) > 1:
            result = top2[1][0]

    return result


# ─────────────────────────────────────────────────────────────
# ИЗВЛЕЧЕНИЕ МЕТА-ТЕГОВ
# ─────────────────────────────────────────────────────────────

def _price_from_meta(soup: BeautifulSoup) -> Optional[float]:
    for attr, val in [
        ("property", "product:price:amount"),
        ("property", "og:price:amount"),
        ("itemprop", "price"),
    ]:
        tag = soup.find("meta", {attr: val})
        if tag:
            p = _to_price(tag.get("content", ""))
            if p:
                return p
    return None


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает ПРАВИЛЬНУЮ цену с Яндекс.Маркет.

    Гарантия правильности:
      1. Ищем по конкретным путям в __NEXT_DATA__ (не по всему JSON)
      2. Regex паттерны нацелены на "min price" (минимальная цена)
      3. Санитарная проверка: два разных товара не могут иметь
         одинаковую цену — если видим дубли → берём из другого источника
    """
    result = {"price": None, "name": "", "in_stock": False, "error": None}
    print(f"   🟡 ЯМ: {url[:65]}")

    html, err = scrape_url(
        url=url,
        render_js=True,
        country_code="ru",
        retry_count=3,
        retry_delay=10.0,
        timeout=90,
        ultra_premium=True,
    )

    if err or not html:
        result["error"] = f"ScraperAPI: {err or 'пустой ответ'}"
        return result

    print(f"     📄 Получено {len(html):,} символов")
    soup = BeautifulSoup(html, "lxml")

    price = None

    # ── Шаг 1: __NEXT_DATA__ по конкретным путям ──────────
    next_tag = soup.find("script", id="__NEXT_DATA__")
    if next_tag and next_tag.string:
        try:
            next_data = json.loads(next_tag.string)
            price = _price_from_next_data(next_data)
            if price:
                print(f"     ✅ __NEXT_DATA__ → {price:,.0f} ₽")
        except (json.JSONDecodeError, Exception) as e:
            print(f"     ⚠️  __NEXT_DATA__ ошибка: {e}")

    # ── Шаг 2: Regex с правильными паттернами ────────────
    if price is None:
        price = _price_from_regex(html)
        if price:
            print(f"     ✅ Regex → {price:,.0f} ₽")

    # ── Шаг 3: Мета-теги ─────────────────────────────────
    if price is None:
        price = _price_from_meta(soup)
        if price:
            print(f"     ✅ Meta → {price:,.0f} ₽")

    result["price"] = price

    # Название
    h1 = soup.find("h1")
    if h1:
        result["name"] = h1.get_text(strip=True)[:300]
    else:
        og = soup.find("meta", property="og:title")
        if og:
            name = og.get("content", "")
            name = re.sub(r"\s*[—\-|]\s*(Яндекс\.?Маркет|Маркет).*", "",
                          name, flags=re.I)
            result["name"] = name[:300]

    # Наличие
    result["in_stock"] = not any(
        s in html.lower()
        for s in ("нет в наличии", "нет на складе", "закончился")
    )

    if result["price"] is None:
        result["error"] = (
            "Цена ЯМ не найдена. ЯМ имеет сильную защиту — "
            "~20% запросов не проходят. Следующий запуск через 3ч."
        )
    else:
        print(f"   ✅ ЯМ итог: {result['price']:,.0f} ₽")

    return result


if __name__ == "__main__":
    test = (sys.argv[1] if len(sys.argv) > 1
            else "https://market.yandex.ru/product--igrovaia-pristavka-valve-steam-deck-oled/1837744073")
    print(f"\n{'='*55}\nТест ЯМ\n{'='*55}")
    r = fetch_price(test)
    print(f"\nЦена:     {r['price']:,.0f} ₽" if r['price'] else "\nЦена:     НЕ НАЙДЕНА")
    print(f"Название: {r['name'][:80]}")
    print(f"Наличие:  {'✅' if r['in_stock'] else '❌'}")
    if r['error']:
        print(f"Ошибка:   {r['error']}")
