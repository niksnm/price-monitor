"""
parsers/yandex_market.py  v4  — Яндекс.Маркет через ScraperAPI
================================================================

ПОЧЕМУ ЦЕНА БЫЛА НЕПРАВИЛЬНОЙ В v3:
  В JSON Яндекс.Маркета огромное количество чисел с ключом "price".
  Старый код брал первое или наиболее частое значение — попадал на:
  - цены похожих товаров от других продавцов
  - исторические цены
  - рекомендованные цены
  - цены в рекламных блоках

КАК ПРАВИЛЬНО НАЙТИ ЦЕНУ:
  ЯМ — это Next.js SPA. Данные хранятся в __NEXT_DATA__ (script-тег).
  Внутри этого JSON есть чёткая структура:

  __NEXT_DATA__.props.pageProps.initialState.report.offers[]
  или
  __NEXT_DATA__.props.pageProps.initialState.productOffers.items[]

  В каждом offer есть:
    prices.min.value — минимальная цена предложения (то что видит пользователь)
    prices.avg.value — средняя цена

  Это то же число что отображается на странице товара.

ДОПОЛНИТЕЛЬНЫЕ СТРАТЕГИИ:
  Если __NEXT_DATA__ не дал результат:
  1. Ищем конкретные паттерны: "minPrice":{"value":74536}
  2. Regex: prices.*?min.*?(\d{4,7}) рядом с ₽
  3. og:price мета-тег (иногда ЯМ его добавляет)
"""

import re
import json
import sys
import os
from collections import Counter
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraping_client import scrape_url


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────────────────────

def _to_price(val) -> Optional[float]:
    """Конвертирует значение в цену (50..10_000_000) или None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        f = float(val)
        return f if 50 <= f <= 10_000_000 else None
    if isinstance(val, str):
        s = re.sub(r'[^\d.,]', '', val.replace('\xa0', '').replace('\u202f', ''))
        if not s:
            return None
        s = s.replace(',', '.')
        parts = s.split('.')
        if len(parts) > 2:
            s = ''.join(parts[:-1]) + '.' + parts[-1]
        try:
            f = float(s)
            return f if 50 <= f <= 10_000_000 else None
        except ValueError:
            return None
    return None


# ─────────────────────────────────────────────────────────────
# МЕТОД 1 — Структурный поиск в __NEXT_DATA__
# ─────────────────────────────────────────────────────────────

def _price_from_next_data(data: dict) -> Optional[float]:
    """
    Обходит __NEXT_DATA__ по известным путям структуры ЯМ
    и возвращает минимальную цену предложения.

    Известные пути (проверяем все, берём первый успешный):
      props.pageProps.initialState.report.offers[].prices.min.value
      props.pageProps.initialState.productOffers.items[].prices.min.value
      props.pageProps.offers[].price.value
    """
    candidates: List[float] = []

    def _walk_for_min_price(obj, depth: int = 0):
        """Ищет паттерн {"min": {"value": N}} или {"minPrice": N}."""
        if depth > 15 or obj is None:
            return
        if isinstance(obj, dict):
            # Паттерн 1: prices.min.value (основной в ЯМ)
            prices_block = obj.get('prices') or obj.get('price')
            if isinstance(prices_block, dict):
                min_block = prices_block.get('min') or prices_block.get('minPrice')
                if isinstance(min_block, dict):
                    pv = min_block.get('value') or min_block.get('amount')
                    p = _to_price(pv)
                    if p:
                        candidates.append(p)
                        return  # Нашли — не идём глубже
                # Прямое price.value
                pv = prices_block.get('value') or prices_block.get('amount')
                p = _to_price(pv)
                if p:
                    candidates.append(p)

            # Паттерн 2: minPrice / minimalPrice напрямую
            for k in ('minPrice', 'minimalPrice', 'lowestPrice'):
                pv = obj.get(k)
                if pv is not None:
                    if isinstance(pv, dict):
                        p = _to_price(pv.get('value') or pv.get('amount'))
                    else:
                        p = _to_price(pv)
                    if p:
                        candidates.append(p)

            for v in obj.values():
                _walk_for_min_price(v, depth + 1)

        elif isinstance(obj, list):
            for item in obj[:50]:
                _walk_for_min_price(item, depth + 1)

    _walk_for_min_price(data)

    if not candidates:
        return None

    # Берём медиану — она ближе всего к реальной цене конкретного товара
    candidates.sort()
    return candidates[len(candidates) // 2]


# ─────────────────────────────────────────────────────────────
# МЕТОД 2 — Regex паттерны специфичные для ЯМ
# ─────────────────────────────────────────────────────────────

def _price_via_regex(html: str) -> Optional[float]:
    """
    Ищет цену через регулярные выражения специфичные для ЯМ.

    Паттерны ЯМ в JSON:
      "min":{"value":74536,...}
      "minPrice":{"value":74536}
      "lowestPrice":{"value":74536}
      "currentPrice":74536

    Приоритет: паттерны с min/lowest → они соответствуют тому
    что пользователь видит на странице.
    """
    patterns = [
        # Самые точные — min цена предложения
        (r'"min"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 3),
        (r'"minPrice"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 3),
        (r'"lowestPrice"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 3),
        (r'"minimalPrice"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 3),
        # Средний приоритет
        (r'"currentPrice"\s*:\s*(\d{4,7})', 2),
        (r'"offerPrice"\s*:\s*(\d{4,7})', 2),
        # Низкий приоритет — могут быть другие числа
        (r'"price"\s*:\s*\{\s*"value"\s*:\s*(\d{4,7})', 1),
    ]

    candidates: List[float] = []
    for pat, weight in patterns:
        for m in re.finditer(pat, html):
            try:
                p = float(m.group(1))
                if 50 <= p <= 10_000_000:
                    candidates.extend([p] * weight)
            except ValueError:
                pass

    if not candidates:
        return None

    return Counter(candidates).most_common(1)[0][0]


# ─────────────────────────────────────────────────────────────
# МЕТОД 3 — Мета-теги
# ─────────────────────────────────────────────────────────────

def _price_via_meta(soup: BeautifulSoup) -> Optional[float]:
    """Пробует мета-теги с ценой."""
    for attr, val in [
        ('property', 'product:price:amount'),
        ('property', 'og:price:amount'),
        ('itemprop', 'price'),
    ]:
        tag = soup.find('meta', {attr: val})
        if tag:
            p = _to_price(tag.get('content', ''))
            if p:
                return p
    return None


# ─────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ ПАРСИНГА
# ─────────────────────────────────────────────────────────────

def fetch_price(url: str) -> Dict[str, Any]:
    """
    Получает ПРАВИЛЬНУЮ цену товара с Яндекс.Маркет.

    Ключевое отличие от v3:
    - Ищем именно prices.min.value (минимальная цена предложения)
    - Это то же число что видит пользователь на странице
    - Не берём рандомные числа с ключом "price"
    """
    result = {'price': None, 'name': '', 'in_stock': False, 'error': None}
    print(f'   🟡 ЯМ: {url[:65]}')

    # Получаем HTML через ScraperAPI с premium прокси
    html, err = scrape_url(
        url=url,
        render_js=True,
        country_code='ru',
        retry_count=3,
        retry_delay=10.0,
        timeout=90,
        ultra_premium=True,
    )

    if err or not html:
        result['error'] = f'ScraperAPI ошибка: {err or "пустой ответ"}'
        return result

    print(f'     📄 Получено {len(html):,} символов')
    soup = BeautifulSoup(html, 'lxml')

    # ── Шаг 1: Ищем __NEXT_DATA__ ────────────────────────
    price = None
    next_data_tag = soup.find('script', id='__NEXT_DATA__')
    if next_data_tag and next_data_tag.string:
        try:
            next_data = json.loads(next_data_tag.string)
            price = _price_from_next_data(next_data)
            if price:
                print(f'     ✅ __NEXT_DATA__ → {price:,.0f} ₽')
        except (json.JSONDecodeError, Exception) as e:
            print(f'     ⚠️  __NEXT_DATA__ не распарсился: {e}')

    # ── Шаг 2: Ищем в других script тегах ────────────────
    if price is None:
        for tag in soup.find_all('script'):
            raw = tag.string or ''
            if len(raw) < 200 or '__NEXT_DATA__' in (tag.get('id') or ''):
                continue
            if '"prices"' in raw or '"minPrice"' in raw or '"lowestPrice"' in raw:
                try:
                    # Пробуем распарсить как JSON если это JSON-blob
                    if raw.strip().startswith('{'):
                        data = json.loads(raw)
                        p = _price_from_next_data(data)
                        if p:
                            price = p
                            print(f'     ✅ Script JSON → {price:,.0f} ₽')
                            break
                except Exception:
                    pass

    # ── Шаг 3: Regex по HTML ──────────────────────────────
    if price is None:
        price = _price_via_regex(html)
        if price:
            print(f'     ✅ Regex → {price:,.0f} ₽')

    # ── Шаг 4: Мета-теги ─────────────────────────────────
    if price is None:
        price = _price_via_meta(soup)
        if price:
            print(f'     ✅ Meta → {price:,.0f} ₽')

    result['price'] = price

    # ── Название ─────────────────────────────────────────
    h1 = soup.find('h1')
    if h1:
        result['name'] = h1.get_text(strip=True)[:300]
    else:
        og = soup.find('meta', property='og:title')
        if og:
            name = og.get('content', '')
            name = re.sub(r'\s*[—\-|]\s*(Яндекс\.?Маркет|Маркет).*', '',
                          name, flags=re.I)
            result['name'] = name[:300]

    # ── Наличие ───────────────────────────────────────────
    low = html.lower()
    result['in_stock'] = not any(
        s in low for s in ('нет в наличии', 'нет на складе', 'закончился')
    )

    if result['price'] is None:
        result['error'] = (
            'Цена ЯМ не найдена. '
            'Яндекс.Маркет — сложная защита, ~20-30% запросов не проходят. '
            'Следующий запуск через 3 часа обычно успешен.'
        )
    else:
        print(f'   ✅ ЯМ итог: {result["price"]:,.0f} ₽')

    return result


if __name__ == '__main__':
    test = (sys.argv[1] if len(sys.argv) > 1
            else 'https://market.yandex.ru/product--smartfon-apple-iphone-15/1837744073')
    print(f'\n{"="*60}\nТест ЯМ парсера\n{"="*60}')
    r = fetch_price(test)
    print(f'\nЦена:     {r["price"]:,.0f} ₽' if r['price'] else '\nЦена:     НЕ НАЙДЕНА')
    print(f'Название: {r["name"][:80]}')
    print(f'Наличие:  {"✅" if r["in_stock"] else "❌"}')
    if r['error']:
        print(f'Ошибка:   {r["error"]}')
