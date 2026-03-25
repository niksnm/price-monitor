"""
database.py — SQLite база данных для истории цен
=================================================

НОВОЕ В v4:
  Добавлены функции для логики «первой цены»:

  is_first_check(product_id) → True если товар ни разу не проверялся
  get_baseline_price(product_id) → первая успешно записанная цена
  get_previous_different_price(product_id, current) → предыдущая цена
                                                       отличная от текущей

ЛОГИКА ПЕРВОЙ ЦЕНЫ:
  При первой проверке нового товара:
  1. Цена сохраняется как обычно
  2. В Telegram отправляется уведомление «Новый товар добавлен»
     с указанием стартовой цены
  3. Все будущие изменения сравниваются С ЭТОЙ ПЕРВОЙ ЦЕНОЙ
     (а не с предыдущим замером)

  Почему именно с первой ценой?
  - Позволяет видеть полный диапазон падения с момента добавления
  - Не теряем информацию о скидке если цена упала давно
  - Пользователь видит «добавил за 8500, сейчас 6900 (-19%)»
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'prices.db')


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Инициализирует БД, создаёт таблицы если их нет."""
    conn = get_connection()
    c = conn.cursor()

    # Таблица истории цен
    c.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id   TEXT    NOT NULL,
            name         TEXT    NOT NULL DEFAULT '',
            url          TEXT    NOT NULL DEFAULT '',
            marketplace  TEXT    NOT NULL DEFAULT '',
            price        REAL,
            currency     TEXT    DEFAULT 'RUB',
            in_stock     INTEGER DEFAULT 1,
            checked_at   TEXT    NOT NULL,
            error        TEXT
        )
    """)

    # Таблица алертов об изменении цены
    c.execute("""
        CREATE TABLE IF NOT EXISTS price_alerts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id      TEXT    NOT NULL,
            alert_type      TEXT    NOT NULL DEFAULT 'drop',
            old_price       REAL    NOT NULL,
            new_price       REAL    NOT NULL,
            change_percent  REAL    NOT NULL,
            alerted_at      TEXT    NOT NULL
        )
    """)

    # Индекс для быстрого поиска по товару + дате
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_history_pid_date
        ON price_history(product_id, checked_at)
    """)

    conn.commit()
    conn.close()
    print('✅ БД инициализирована')


# ─────────────────────────────────────────────────────────────
# ЗАПИСЬ
# ─────────────────────────────────────────────────────────────

def save_price(product_id: str, name: str, url: str, marketplace: str,
               price: Optional[float], in_stock: bool = True,
               error: Optional[str] = None):
    """Сохраняет замер цены в историю."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO price_history
            (product_id, name, url, marketplace, price, in_stock, checked_at, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        product_id, name or '', url or '', marketplace or '',
        price, int(bool(in_stock)),
        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        error
    ))
    conn.commit()
    conn.close()


def save_alert(product_id: str, old_price: float, new_price: float,
               change_percent: float, alert_type: str = 'drop'):
    """
    Сохраняет запись об алерте.
    alert_type: 'drop' (падение) или 'new_product' (первая цена)
    """
    conn = get_connection()
    conn.execute("""
        INSERT INTO price_alerts
            (product_id, alert_type, old_price, new_price, change_percent, alerted_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        product_id, alert_type, old_price, new_price, change_percent,
        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    ))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────
# ЧТЕНИЕ — базовые
# ─────────────────────────────────────────────────────────────

def get_last_price(product_id: str) -> Optional[Dict[str, Any]]:
    """Последняя успешная цена товара."""
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY checked_at DESC LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_price_history(product_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    """История цен товара, последние limit записей, по возрастанию даты."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY checked_at DESC LIMIT ?
    """, (product_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def get_all_latest_prices() -> List[Dict[str, Any]]:
    """Последние цены для всех товаров (для дашборда)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT ph.*
        FROM price_history ph
        INNER JOIN (
            SELECT product_id, MAX(checked_at) AS mx
            FROM price_history WHERE price IS NOT NULL
            GROUP BY product_id
        ) t ON ph.product_id = t.product_id AND ph.checked_at = t.mx
        ORDER BY ph.marketplace, ph.name
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats() -> Dict[str, Any]:
    """Общая статистика."""
    conn = get_connection()
    res = {
        'total_checks':    conn.execute('SELECT COUNT(*) FROM price_history').fetchone()[0],
        'total_alerts':    conn.execute('SELECT COUNT(*) FROM price_alerts').fetchone()[0],
        'tracked_products': conn.execute(
            'SELECT COUNT(DISTINCT product_id) FROM price_history').fetchone()[0],
        'last_check':      conn.execute(
            'SELECT MAX(checked_at) FROM price_history').fetchone()[0],
    }
    conn.close()
    return res


# ─────────────────────────────────────────────────────────────
# ЧТЕНИЕ — логика первой цены (новое в v4)
# ─────────────────────────────────────────────────────────────

def is_first_check(product_id: str) -> bool:
    """
    Возвращает True если у этого товара ещё НЕТ ни одной записи в БД.

    Используется для определения «нового» товара:
    - True  → товар добавлен только что, эта проверка первая
    - False → товар уже проверялся раньше, есть история

    Важно: проверяем ДО сохранения новой цены.
    """
    conn = get_connection()
    count = conn.execute(
        'SELECT COUNT(*) FROM price_history WHERE product_id = ?',
        (product_id,)
    ).fetchone()[0]
    conn.close()
    return count == 0


def get_baseline_price(product_id: str) -> Optional[Dict[str, Any]]:
    """
    Возвращает ПЕРВУЮ успешно записанную цену товара.

    Это «базовая» цена — с ней сравниваются все последующие.
    Именно она показывается в уведомлениях как «цена на момент добавления».

    Отличие от get_last_price:
    - get_last_price → ПОСЛЕДНЯЯ цена (для сравнения «сейчас vs вчера»)
    - get_baseline_price → ПЕРВАЯ цена (для сравнения «сейчас vs начало»)
    """
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY checked_at ASC LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_previous_different_price(product_id: str,
                                  current_price: float) -> Optional[Dict[str, Any]]:
    """
    Возвращает последнюю цену ОТЛИЧНУЮ от текущей.

    Нужно чтобы не сравнивать цену саму с собой когда она не менялась.

    Пример:
      Замер 1: 8500 ₽
      Замер 2: 8500 ₽  ← не изменилась, пропускаем
      Замер 3: 7200 ₽  ← изменилась! Сравниваем 7200 vs 8500 = -15.3%
    """
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ?
          AND price IS NOT NULL
          AND ABS(price - ?) > 0.01
        ORDER BY checked_at DESC LIMIT 1
    """, (product_id, current_price)).fetchone()
    conn.close()
    return dict(row) if row else None


def count_successful_checks(product_id: str) -> int:
    """
    Возвращает количество успешных проверок цены товара.

    Используется для статистики и для определения
    «достаточно ли данных для сравнения».
    """
    conn = get_connection()
    count = conn.execute(
        'SELECT COUNT(*) FROM price_history WHERE product_id = ? AND price IS NOT NULL',
        (product_id,)
    ).fetchone()[0]
    conn.close()
    return count
