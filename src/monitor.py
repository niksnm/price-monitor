"""
monitor.py — Главный скрипт мониторинга цен (версия с ScraperAPI)
==================================================================

ЗАПУСКАЕТСЯ: GitHub Actions каждые 3 часа автоматически

ЧТО ДЕЛАЕТ:
  1. Проверяет баланс ScraperAPI (чтобы знать что ключ работает)
  2. Читает список товаров из config/products.json
  3. Для каждого товара:
     а) Вызывает нужный парсер (WB/Ozon/ЯМ)
     б) Сохраняет цену в SQLite базу данных
     в) Сравнивает с предыдущей ценой
     г) Если цена упала на threshold% — отправляет в Telegram
  4. Генерирует HTML дашборд для GitHub Pages
  5. Выводит итоговую статистику в лог
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.dirname(__file__))

from database import init_db, save_price, get_last_price, save_alert, get_stats
from notifier.telegram import send_price_drop_alert, send_message
from parsers import wildberries, ozon, yandex_market
from scraping_client import check_account_status, print_session_stats


CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config", "products.json"
)


def load_config() -> Dict[str, Any]:
    """Загружает конфигурацию из products.json."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_parser(marketplace: str):
    """Возвращает модуль парсера по имени маркетплейса."""
    mapping = {
        "wildberries": wildberries,
        "ozon":        ozon,
        "yandex_market": yandex_market,
        "ym":          yandex_market,
    }
    return mapping.get(marketplace.lower())


def fmt(price: Optional[float]) -> str:
    """Форматирует цену для вывода в лог."""
    if price is None:
        return "н/д"
    return f"{price:>10,.0f} ₽".replace(",", " ")


def get_previous_price(product_id: str, current_price: float) -> Optional[Dict]:
    """
    Возвращает ПРЕДЫДУЩУЮ цену товара (не текущую).

    Логика: ищем последнюю успешную запись с ценой ОТЛИЧНОЙ от текущей.
    Это нужно чтобы отслеживать реальное изменение, а не сравнивать
    цену саму с собой.
    """
    from database import get_connection
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ?
          AND price IS NOT NULL
          AND ABS(price - ?) > 0.01
        ORDER BY checked_at DESC
        LIMIT 1
    """, (product_id, current_price)).fetchone()
    conn.close()
    return dict(row) if row else None


def check_single_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Проверяет один товар: получает цену, сохраняет, проверяет алерт.

    ВОЗВРАЩАЕТ:
      {
        "success": True/False,
        "alert_sent": True/False,
        "price": float или None,
        "change_percent": float или None,
        "error": str или None
      }
    """
    pid        = product["id"]
    name       = product["name"]
    url        = product["url"]
    marketplace = product["marketplace"]
    threshold  = product.get("alert_threshold", 10)

    print(f"\n  📦 {name}")
    print(f"     URL: {url[:65]}...")
    print(f"     Маркетплейс: {marketplace} | Порог: {threshold}%")

    # Выбираем парсер
    parser = get_parser(marketplace)
    if not parser:
        msg = f"Неизвестный маркетплейс: {marketplace}"
        print(f"     ❌ {msg}")
        save_price(pid, name, url, marketplace, None, False, msg)
        return {"success": False, "error": msg}

    # Запрашиваем цену
    result = parser.fetch_price(url)
    new_price = result.get("price")
    in_stock  = result.get("in_stock", False)
    error     = result.get("error")

    # Обновляем имя если парсер нашёл более точное
    if not name and result.get("name"):
        name = result["name"]

    print(f"     Цена:     {fmt(new_price)}")
    print(f"     Наличие:  {'✅ Есть' if in_stock else '❌ Нет'}")
    if error:
        print(f"     ⚠️  {error[:100]}")

    # Сохраняем в БД
    save_price(pid, name, url, marketplace, new_price, in_stock, error)

    # Проверяем изменение цены
    if new_price is not None:
        prev_record = get_previous_price(pid, new_price)

        if prev_record and prev_record.get("price"):
            old_price = prev_record["price"]
            pct_change = ((new_price - old_price) / old_price) * 100

            if pct_change < 0:
                print(f"     📉 Изменение: {pct_change:+.1f}% "
                      f"({fmt(old_price)} → {fmt(new_price)})")

                # Проверяем достигнут ли порог
                if abs(pct_change) >= threshold:
                    print(f"     🚨 ПОРОГ СРАБОТАЛ! ({threshold}%) Отправляем уведомление...")
                    save_alert(pid, old_price, new_price, pct_change)

                    sent = send_price_drop_alert(
                        product_name=name,
                        marketplace=marketplace,
                        old_price=old_price,
                        new_price=new_price,
                        change_percent=pct_change,
                        url=url,
                        threshold=threshold
                    )
                    if sent:
                        print(f"     ✅ Telegram уведомление отправлено!")
                    return {
                        "success": True, "alert_sent": True,
                        "price": new_price, "change_percent": pct_change
                    }

            elif pct_change > 0:
                print(f"     📈 Цена выросла на {pct_change:.1f}%")
            else:
                print(f"     ➡️  Цена не изменилась")

        elif prev_record is None:
            print(f"     📌 Первая запись для этого товара")

    return {
        "success": new_price is not None,
        "alert_sent": False,
        "price": new_price,
        "change_percent": None,
        "error": error
    }


def run_monitoring():
    """
    ТОЧКА ВХОДА — запускает полный цикл мониторинга.
    Вызывается из GitHub Actions.
    """
    print("\n" + "═" * 65)
    print("🚀 ЗАПУСК МОНИТОРИНГА ЦЕН")
    print(f"⏰ {datetime.now().strftime('%d.%m.%Y %H:%M:%S')} UTC")
    print("═" * 65)

    # Инициализация БД
    init_db()

    # ── Проверяем ScraperAPI ──────────────────────────────────
    print("\n📡 Проверка ScraperAPI аккаунта...")
    api_status = check_account_status()

    if not api_status["ok"]:
        print(f"   ❌ Ошибка: {api_status['error']}")
        print("   ⚠️  Проверьте SCRAPER_API_KEY в GitHub Secrets")
        # Продолжаем — WB работает без ScraperAPI
    else:
        left = api_status.get("requests_left", 0)
        limit = api_status.get("requests_limit", 0)
        print(f"   ✅ Аккаунт активен")
        print(f"   📊 Запросов: использовано {api_status.get('requests_used', 0)}, "
              f"осталось {left} из {limit}")

        # Предупреждение если запросов мало
        if left < 50:
            warn = f"⚠️ Осталось мало запросов ScraperAPI: {left}!"
            print(f"   {warn}")
            send_message(warn)

    # ── Загружаем товары ──────────────────────────────────────
    config   = load_config()
    products = [p for p in config["products"] if p.get("active", True)]
    print(f"\n📋 Товаров для проверки: {len(products)}")

    # ── Проверяем каждый товар ────────────────────────────────
    stats = {"checked": 0, "ok": 0, "errors": 0, "alerts": 0}

    for i, product in enumerate(products, 1):
        print(f"\n[{i}/{len(products)}]", end="")

        try:
            result = check_single_product(product)
            stats["checked"] += 1

            if result.get("success"):
                stats["ok"] += 1
            else:
                stats["errors"] += 1

            if result.get("alert_sent"):
                stats["alerts"] += 1

        except Exception as exc:
            stats["errors"] += 1
            print(f"\n     💥 Критическая ошибка: {exc}")
            import traceback
            traceback.print_exc()

        # Пауза между запросами — вежливо и даём ScraperAPI отдохнуть
        if i < len(products):
            pause = 5 if product.get("marketplace") == "wildberries" else 15
            print(f"     💤 Пауза {pause}с перед следующим товаром...")
            time.sleep(pause)

    # ── Генерируем дашборд ────────────────────────────────────
    print("\n📊 Генерация дашборда...")
    try:
        from dashboard_generator import generate_dashboard
        generate_dashboard()
        print("   ✅ Дашборд обновлён")
    except Exception as e:
        print(f"   ⚠️  Ошибка генерации дашборда: {e}")

    # ── Итоговая статистика ───────────────────────────────────
    print_session_stats()

    print("\n" + "═" * 65)
    print("📈 ИТОГИ СЕССИИ:")
    print(f"   ✅ Проверено:  {stats['checked']} товаров")
    print(f"   ✅ Успешно:    {stats['ok']}")
    print(f"   ❌ Ошибок:     {stats['errors']}")
    print(f"   🚨 Алертов:    {stats['alerts']}")
    print("═" * 65 + "\n")

    return stats


if __name__ == "__main__":
    run_monitoring()
