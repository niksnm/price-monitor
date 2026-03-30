"""
database.py — SQLite база данных для истории цен
=================================================

ИСПРАВЛЕНИЕ КРИТИЧЕСКОГО БАГА v4 → v4.1:
  Ошибка: "table price_alerts has no column named a"

  ПРИЧИНА:
    Старая база данных (prices.db) была создана предыдущей версией кода
    без колонки alert_type. Новый код пытается вставить запись с этой
    колонкой — SQLite выдаёт ошибку и весь мониторинг ломается.

  ИСПРАВЛЕНИЕ:
    В функции init_db() добавлена МИГРАЦИЯ — после создания таблицы
    скрипт пытается добавить недостающие колонки через ALTER TABLE.
    Если колонка уже есть — ошибка перехватывается и игнорируется.
    Если колонки нет — добавляется с правильным DEFAULT значением.

    Эта техника называется "безопасная миграция схемы" и позволяет
    обновлять код не удаляя существующую базу данных.

СТРУКТУРА ТАБЛИЦ:
  price_history — все замеры цен по всем товарам
  price_alerts  — история отправленных уведомлений
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'prices.db')


def get_connection() -> sqlite3.Connection:
    """Подключается к БД. Создаёт директорию data/ если её нет."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _migrate(conn: sqlite3.Connection):
    """
    МИГРАЦИЯ СХЕМЫ БД.

    Безопасно добавляет колонки которых может не быть в старой БД.
    Паттерн: try ALTER TABLE → except если колонка уже есть — пропустить.

    Это решает ошибку:
      "table price_alerts has no column named alert_type"
    которая возникает когда старая prices.db встречает новый код.
    """
    c = conn.cursor()

    # Список миграций: (таблица, колонка, определение)
    migrations = [
        # price_alerts — добавляем alert_type если нет
        ("price_alerts", "alert_type",
         "ALTER TABLE price_alerts ADD COLUMN alert_type TEXT NOT NULL DEFAULT 'drop'"),

        # price_history — на случай если name/url/marketplace были NOT NULL без DEFAULT
        # (старые версии могли создать таблицу иначе)
    ]

    for table, column, sql in migrations:
        # Проверяем существует ли колонка
        try:
            existing = c.execute(f"PRAGMA table_info({table})").fetchall()
            existing_cols = [row[1] for row in existing]
            if column not in existing_cols:
                c.execute(sql)
                print(f"   ✅ Миграция: добавлена колонка {table}.{column}")
            # Если колонка есть — просто пропускаем
        except sqlite3.OperationalError as e:
            # Таблица может не существовать ещё — это нормально
            if "no such table" not in str(e):
                print(f"   ⚠️  Миграция {table}.{column}: {e}")

    conn.commit()


def init_db():
    """
    Инициализирует БД.

    Порядок действий:
    1. Создаём таблицы если не существуют (CREATE TABLE IF NOT EXISTS)
    2. Запускаем миграции для добавления новых колонок в старые таблицы
    3. Создаём индексы для быстрого поиска

    Безопасно вызывать при каждом старте — повторный вызов ничего не сломает.
    """
    conn = get_connection()
    c = conn.cursor()

    # ── Таблица истории цен ───────────────────────────────
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

    # ── Таблица алертов ───────────────────────────────────
    # alert_type: 'drop' = падение цены, 'new_product' = первое добавление
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

    # ── Индекс для быстрого поиска ────────────────────────
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_history_pid_date
        ON price_history(product_id, checked_at)
    """)

    conn.commit()

    # ── Запускаем миграции (КРИТИЧЕСКИ ВАЖНО) ────────────
    # Добавляет колонки которых нет в старой БД
    _migrate(conn)

    conn.close()
    print('✅ БД инициализирована (миграции выполнены)')


# ─────────────────────────────────────────────────────────────
# ЗАПИСЬ
# ─────────────────────────────────────────────────────────────

def save_price(product_id: str, name: str, url: str, marketplace: str,
               price: Optional[float], in_stock: bool = True,
               error: Optional[str] = None):
    """Сохраняет один замер цены в таблицу price_history."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO price_history
            (product_id, name, url, marketplace, price, in_stock, checked_at, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        product_id,
        name or '',
        url or '',
        marketplace or '',
        price,
        int(bool(in_stock)),
        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        error,
    ))
    conn.commit()
    conn.close()


def save_alert(product_id: str, old_price: float, new_price: float,
               change_percent: float, alert_type: str = 'drop'):
    """
    Сохраняет запись об отправленном алерте.

    alert_type:
      'drop'        — цена упала ниже порога
      'new_product' — товар добавлен в мониторинг (первая цена)
    """
    conn = get_connection()
    conn.execute("""
        INSERT INTO price_alerts
            (product_id, alert_type, old_price, new_price, change_percent, alerted_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        product_id,
        alert_type,
        old_price,
        new_price,
        change_percent,
        datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    ))
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────
# ЧТЕНИЕ — базовые
# ─────────────────────────────────────────────────────────────

def get_last_price(product_id: str) -> Optional[Dict[str, Any]]:
    """Последняя успешная запись цены для товара."""
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY checked_at DESC LIMIT 1
    """, (product_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_price_history(product_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    """История цен товара за последние limit замеров (по возрастанию даты)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM price_history
        WHERE product_id = ? AND price IS NOT NULL
        ORDER BY checked_at DESC LIMIT ?
    """, (product_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


def get_all_latest_prices() -> List[Dict[str, Any]]:
    """Последние цены всех товаров (для дашборда)."""
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
    """Общая статистика по БД."""
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
# ЧТЕНИЕ — логика первой цены
# ─────────────────────────────────────────────────────────────

def is_first_check(product_id: str) -> bool:
    """
    True если у этого product_id ещё НЕТ ни одной записи в БД.
    Вызывать ДО save_price чтобы определить первый запуск.
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
    Первая успешно записанная цена товара (базовая/стартовая).
    Все последующие изменения сравниваются с ней.
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
    Последняя цена отличная от current_price.
    Нужна чтобы не сравнивать цену саму с собой.
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
    """Количество успешных замеров цены для товара."""
    conn = get_connection()
    count = conn.execute(
        'SELECT COUNT(*) FROM price_history WHERE product_id = ? AND price IS NOT NULL',
        (product_id,)
    ).fetchone()[0]
    conn.close()
    return count
