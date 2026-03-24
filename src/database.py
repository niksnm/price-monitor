"""
database.py — управление SQLite базой данных для хранения истории цен
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "prices.db")


def get_connection() -> sqlite3.Connection:
    """Создаёт подключение к БД, создаёт директорию если нет."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Инициализация БД — создание таблиц если не существуют."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id  TEXT    NOT NULL,
            name        TEXT    NOT NULL,
            url         TEXT    NOT NULL,
            marketplace TEXT    NOT NULL,
            price       REAL,
            currency    TEXT    DEFAULT 'RUB',
            in_stock    INTEGER DEFAULT 1,
            checked_at  TEXT    NOT NULL,
            error       TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_alerts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id      TEXT    NOT NULL,
            old_price       REAL    NOT NULL,
            new_price       REAL    NOT NULL,
            change_percent  REAL    NOT NULL,
            alerted_at      TEXT    NOT NULL,
            notified        INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_history_product
        ON price_history(product_id, checked_at)
    """)

    conn.commit()
    conn.close()
    print("✅ БД инициализирована")


def save_price(product_id: str, name: str, url: str, marketplace: str,
               price: Optional[float], in_stock: bool = True, error: str = None):
    """Сохраняет новую запись цены в историю."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO price_history
            (product_id, name, url, marketplace, price, in_stock, checked_at, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        product_id, name, url, marketplace,
        price, int(in_stock),
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        error
    ))
    conn.commit()
    conn.close()


def get_last_price(product_id: str) -> Optional[Dict[str, Any]]:
    """Возвращает последнюю успешную цену товара."""
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY checked_at DESC
        LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_price_history(product_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Возвращает историю цен товара (последние N записей)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY checked_at DESC
        LIMIT ?
    """, (product_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def get_all_latest_prices() -> List[Dict[str, Any]]:
    """Возвращает последние цены для всех товаров."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT ph.*
        FROM price_history ph
        INNER JOIN (
            SELECT product_id, MAX(checked_at) AS max_date
            FROM price_history
            WHERE price IS NOT NULL
            GROUP BY product_id
        ) latest ON ph.product_id = latest.product_id
                    AND ph.checked_at = latest.max_date
        ORDER BY ph.marketplace, ph.name
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_alert(product_id: str, old_price: float, new_price: float,
               change_percent: float):
    """Сохраняет запись об алерте."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO price_alerts
            (product_id, old_price, new_price, change_percent, alerted_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        product_id, old_price, new_price, change_percent,
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ))
    conn.commit()
    conn.close()


def get_stats() -> Dict[str, Any]:
    """Возвращает общую статистику по мониторингу."""
    conn = get_connection()
    total_checks = conn.execute(
        "SELECT COUNT(*) FROM price_history"
    ).fetchone()[0]
    total_alerts = conn.execute(
        "SELECT COUNT(*) FROM price_alerts"
    ).fetchone()[0]
    tracked_products = conn.execute(
        "SELECT COUNT(DISTINCT product_id) FROM price_history"
    ).fetchone()[0]
    last_check = conn.execute(
        "SELECT MAX(checked_at) FROM price_history"
    ).fetchone()[0]
    conn.close()
    return {
        "total_checks": total_checks,
        "total_alerts": total_alerts,
        "tracked_products": tracked_products,
        "last_check": last_check
    }
