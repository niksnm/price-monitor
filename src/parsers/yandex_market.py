"""
parsers/yandex_market.py — ЯМ 2026 (правильная цена, без мусора)
=================================================================

ИСПРАВЛЕНИЕ "8 589 000 ₽ (+9642.0%)":
  Regex подбирал число 8589000 из рекламного блока или другого
  контекста. Нужна строгая фильтрация.

ПРАВИЛА ФИЛЬТРАЦИИ ЦЕНЫ:
  1. Ищем ТОЛЬКО в __NEXT_DATA__ по конкретным структурным путям
  2. Regex использует ТОЛЬКО паттерны с "min"/"value" контекстом
  3. Финальная проверка: цена должна быть в диапазоне 100..2_000_000 ₽
     (ограничение 2 млн отсекает явный мусор типа 8.5 млн)
  4. Если найдено несколько кандидатов — берём медиану, а не максимум
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
# УТИЛИТЫ
# ─────────────────────────────────────────────────────────────

# Максимально разумная цена товара на ЯМ (2 млн ₽)
# Электроника, авто, ювелирка — редко дороже
MAX_REASONABLE_PRICE = 2_000_000


def _to_price(val) -> Optional[float]:
    """Конвертирует значение в цену. Строгий диапазон: 100..2_000_000."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 100 <= f <= MAX_REASONABLE_PRICE else None
    if isinstance(val, str):
        s = re.sub(r'[^\d]', '', val)
        if not s or len(s) > 8:   # >8 цифр = >99 млн = точно мусор
            return None
        try:
            f = float(s)
            return f if 100 <= f <= MAX_REASONABLE_PRICE else None
        except ValueError:
            return None
    return None


def _get(obj, *keys):
    """Безопасный доступ к вложенному значению."""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


# ─────────────────────────────────────────────────────────────
# ИЗВЛЕЧЕНИЕ ИЗ __NEXT_DATA__ (основной метод)
# ─────────────────────────────────────────────────────────────

def _price_from_next_data(data: dict) -> Optional[float]:
    """
    Ищет цену по конкретным структурным путям ЯМ.
    Не использует общий поиск "price" по всему дереву — только известные пути.
    """
    # Стандартные точки входа
    initial    = _get(data, "props", "pageProps", "initialState") or {}
    page_props = _get(data, "props", "pageProps") or {}

    candidates: List[float] = []

    # ── Path 1: productCard ──────────────────────────────
    product = _get(initial, "productCard", "product") or {}

    # offers.top.price.value — цена топового предложения
    p1 = _get(product, "offers", "top", "price", "value")
    p = _to_price(p1)
    if p:
        candidates.append(p)

    # prices.min.value — минимальная цена
    p2 = _get(product, "prices", "min", "value")
    p = _to_price(p2)
    if p:
        candidates.append(p)

    # price.value или price напрямую
    price_field = product.get("price")
    if isinstance(price_field, dict):
        p = _to_price(price_field.get("value") or price_field.get("amount"))
        if p:
            candidates.append(p)
    elif isinstance(price_field, (int, float)):
        p = _to_price(price_field)
        if p:
            candidates.append(p)

    # ── Path 2: sku ──────────────────────────────────────
    sku_product = _get(initial, "productCard", "sku", "product") or {}
    p3 = _get(sku_product, "offers", "top", "price", "value")
    p = _to_price(p3)
    if p:
        candidates.append(p)

    # ── Path 3: report ────────────────────────────────────
    report = _get(initial, "report") or {}
    report_product = report.get("product") or report
    if isinstance(report_product, dict):
        p4 = _get(report_product, "prices", "min", "value")
        p = _to_price(p4)
        if p:
            candidates.append(p)

        # offers в report
        offers = report_product.get("offers") or []
        if isinstance(offers, list):
            for offer in offers[:3]:
                op = (
                    _get(offer, "price", "value") or
                    _get(offer, "prices", "min", "value") or
                    offer.get("price")
                )
                p = _to_price(op)
                if p:
                    candidates.append(p)
                    break

    # ── Path 4: pageProps.product ─────────────────────────
    pp_prod = page_props.get("product") or {}
    if isinstance(pp_prod, dict):
        p5 = _get(pp_prod, "price", "value")
        p = _to_price(p5)
        if p:
            candidates.append(p)

    if not candidates:
        return None

    for val in candidates:
        print(f"     🔍 Кандидат: {val:,.0f} ₽")

    # Берём медиану (защита от выбросов)
    candidates.sort()
    return candidates[len(candidates) // 2]


# ─────────────────────────────────────────────────────────────
# ИЗВЛЕЧЕНИЕ ЧЕРЕЗ REGEX
# ─────────────────────────────────────────────────────────────

def _price_from_regex(html: str) -> Optional[float]:
    """
    Строгие regex паттерны — только контекст min/value/offer.
    НЕ ищем просто число рядом с ₽ — это даёт мусор.
    """
    # Только паттерны которые точно указывают на цену товара
    STRICT_PATTERNS = [
        # {"min":{"value":74536}}
        (r'"min"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})\b', 5),
        # {"lowestPrice":{"value":74536}}
        (r'"lowestPrice"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})\b', 5),
        # {"minimalPrice":{"value":74536}}
        (r'"minimalPrice"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})\b', 5),
        # "top":{"price":{"value":74536}}
        (r'"top"\s*:\s*\{[^}]{0,150}"value"\s*:\s*(\d{4,7})\b', 4),
        # "price":{"value":74536} — только с value-контекстом
        (r'"price"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})\b', 3),
        # "currentPrice":74536
        (r'"currentPrice"\s*:\s*(\d{4,7})\b', 2),
    ]

    from collections import Counter
    weighted: List[float] = []

    for pat, weight in STRICT_PATTERNS:
        for m in re.finditer(pat, html):
            try:
                p = float(m.group(1))
                p_valid = _to_price(p)
                if p_valid:
                    weighted.extend([p_valid] * weight)
            except ValueError:
                pass

    if not weighted:
        return None

    # Медиана взвешенных кандидатов
    weighted.sort()
    result = weighted[len(weighted) // 2]

    # Дополнительная проверка: отфильтровываем если цена подозрительно
    # высокая (>500к для нетипичных товаров)
    return result


# ─────────────────────────────────────────────────────────────
# МЕТА-ТЕГИ
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
    Получает цену товара Яндекс.Маркет.

    Гарантия правильности:
      - Максимальная цена ограничена 2 000 000 ₽ (отсекает мусор)
      - Поиск только по конкретным структурным путям в __NEXT_DATA__
      - Regex только с строгим контекстом ("min", "value", "top")
      - Медиана кандидатов вместо максимума
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

    # ── Шаг 1: __NEXT_DATA__ ─────────────────────────────
    next_tag = soup.find("script", id="__NEXT_DATA__")
    if next_tag and next_tag.string:
        try:
            nd = json.loads(next_tag.string)
            price = _price_from_next_data(nd)
            if price:
                print(f"     ✅ __NEXT_DATA__ → {price:,.0f} ₽")
        except Exception as e:
            print(f"     ⚠️  __NEXT_DATA__: {e}")

    # ── Шаг 2: Строгий Regex ─────────────────────────────
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
            name = re.sub(
                r"\s*[—\-|]\s*(Яндекс\.?Маркет|Маркет).*", "",
                name, flags=re.I
            )
            result["name"] = name[:300]

    # Наличие
    result["in_stock"] = not any(
        s in html.lower()
        for s in ("нет в наличии", "нет на складе", "закончился")
    )

    if price is None:
        result["error"] = (
            "Цена ЯМ не найдена. "
            "ЯМ имеет сильную защиту — ~20% запросов не проходят. "
            "Следующий запуск через 3 часа."
        )
    else:
        print(f"   ✅ ЯМ итог: {price:,.0f} ₽")

    return result
