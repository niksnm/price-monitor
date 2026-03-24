"""
dashboard_generator.py — генератор HTML дашборда для GitHub Pages
Создаёт docs/index.html с таблицей цен, графиками и историей изменений.
"""

import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from database import get_all_latest_prices, get_price_history, get_stats, get_connection

DOCS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "products.json")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_price_change(product_id: str):
    """Возвращает % изменения цены относительно предыдущего замера."""
    history = get_price_history(product_id, limit=2)
    if len(history) < 2:
        return None
    old = history[0]["price"]
    new = history[-1]["price"]
    if old and new and old != 0:
        return ((new - old) / old) * 100
    return None


def build_chart_data(product_id: str):
    """Формирует данные для графика Chart.js."""
    history = get_price_history(product_id, limit=30)
    labels = [h["checked_at"][:10] for h in history]
    prices = [h["price"] for h in history]
    return {"labels": labels, "prices": prices}


def fmt_price(price):
    if price is None:
        return "—"
    return f"{price:,.0f} ₽".replace(",", "\u202f")


def generate_dashboard():
    os.makedirs(DOCS_DIR, exist_ok=True)

    config = load_config()
    products_cfg = {p["id"]: p for p in config["products"]}
    latest = get_all_latest_prices()
    stats = get_stats()

    # Собираем данные по каждому товару
    products_data = []
    for row in latest:
        pid = row["product_id"]
        cfg = products_cfg.get(pid, {})
        change = get_price_change(pid)
        chart = build_chart_data(pid)
        products_data.append({
            "id": pid,
            "name": row["name"] or cfg.get("name", pid),
            "url": row["url"],
            "marketplace": row["marketplace"],
            "price": row["price"],
            "in_stock": bool(row["in_stock"]),
            "checked_at": row["checked_at"],
            "change_percent": change,
            "threshold": cfg.get("alert_threshold", 10),
            "chart": chart,
            "notes": cfg.get("notes", ""),
        })

    # Все chart данные как JS объект
    charts_js = json.dumps({p["id"]: p["chart"] for p in products_data}, ensure_ascii=False)

    mp_badge = {
        "wildberries": ('<span class="badge wb">WB</span>', "#cb11ab"),
        "ozon": ('<span class="badge oz">OZON</span>', "#005bff"),
        "yandex_market": ('<span class="badge ym">ЯМ</span>', "#fc3f1d"),
    }

    rows_html = ""
    for p in products_data:
        change = p["change_percent"]
        if change is None:
            change_html = '<span class="change neutral">—</span>'
        elif change < 0:
            change_html = f'<span class="change down">▼ {abs(change):.1f}%</span>'
        elif change > 0:
            change_html = f'<span class="change up">▲ {change:.1f}%</span>'
        else:
            change_html = '<span class="change neutral">→ 0%</span>'

        badge, _ = mp_badge.get(p["marketplace"], ('<span class="badge">?</span>', "#888"))
        stock = '✅' if p["in_stock"] else '❌'
        price_str = fmt_price(p["price"])
        ts = p["checked_at"][:16].replace("T", " ") if p["checked_at"] else "—"

        rows_html += f"""
        <tr onclick="showChart('{p['id']}')" class="product-row" title="Нажмите для просмотра графика">
          <td class="td-name">
            <a href="{p['url']}" target="_blank" class="product-link">{p['name'][:55]}</a>
            {f'<div class="notes">{p["notes"]}</div>' if p["notes"] else ''}
          </td>
          <td>{badge}</td>
          <td class="td-price"><b>{price_str}</b></td>
          <td>{change_html}</td>
          <td>{stock}</td>
          <td class="td-threshold">{p['threshold']}%</td>
          <td class="td-time">{ts}</td>
        </tr>"""

    # Карточки с мини-статистикой
    last_upd = stats.get("last_check", "—")
    if last_upd and last_upd != "—":
        last_upd = last_upd[:16]

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Мониторинг цен</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg: #0d0f14;
    --surface: #151820;
    --surface2: #1c2030;
    --border: #252a3a;
    --accent: #4f8eff;
    --accent2: #7c5cfc;
    --green: #22c55e;
    --red: #ef4444;
    --yellow: #f59e0b;
    --text: #e2e8f0;
    --muted: #64748b;
    --wb: #cb11ab;
    --oz: #005bff;
    --ym: #fc3f1d;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{
    font-family: 'Manrope', sans-serif;
    background: var(--bg);
    color: var(--text);
    min-height: 100vh;
  }}

  /* Header */
  header {{
    background: linear-gradient(135deg, #0d0f14 0%, #151828 100%);
    border-bottom: 1px solid var(--border);
    padding: 24px 40px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
    backdrop-filter: blur(12px);
  }}
  .logo {{
    display: flex;
    align-items: center;
    gap: 12px;
  }}
  .logo-icon {{
    width: 40px; height: 40px;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 20px;
  }}
  .logo-text {{ font-size: 18px; font-weight: 800; letter-spacing: -0.5px; }}
  .logo-sub {{ font-size: 12px; color: var(--muted); font-weight: 500; }}
  .last-update {{
    font-size: 12px;
    color: var(--muted);
    font-family: 'JetBrains Mono', monospace;
    background: var(--surface2);
    padding: 8px 16px;
    border-radius: 20px;
    border: 1px solid var(--border);
  }}
  .last-update span {{ color: var(--green); }}

  /* Stats cards */
  .stats-row {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    padding: 24px 40px;
  }}
  .stat-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 20px 24px;
    position: relative;
    overflow: hidden;
  }}
  .stat-card::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--accent), var(--accent2));
  }}
  .stat-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase;
                  letter-spacing: 1px; font-weight: 600; margin-bottom: 8px; }}
  .stat-value {{ font-size: 28px; font-weight: 800; font-family: 'JetBrains Mono', monospace; }}
  .stat-sub {{ font-size: 11px; color: var(--muted); margin-top: 4px; }}

  /* Main content */
  .content {{ padding: 0 40px 40px; }}

  /* Chart panel */
  .chart-panel {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 24px;
    margin-bottom: 24px;
    display: none;
  }}
  .chart-panel.active {{ display: block; }}
  .chart-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
  }}
  .chart-title {{ font-size: 16px; font-weight: 700; }}
  .chart-close {{
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--muted);
    width: 32px; height: 32px;
    border-radius: 8px;
    cursor: pointer;
    font-size: 16px;
    display: flex; align-items: center; justify-content: center;
  }}
  .chart-close:hover {{ color: var(--text); border-color: var(--accent); }}
  canvas {{ max-height: 260px; }}

  /* Table */
  .table-wrap {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    overflow: hidden;
  }}
  .table-header {{
    padding: 20px 24px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: space-between;
  }}
  .table-title {{ font-size: 15px; font-weight: 700; }}
  .table-hint {{ font-size: 12px; color: var(--muted); }}

  table {{ width: 100%; border-collapse: collapse; }}
  thead th {{
    padding: 12px 16px;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--muted);
    font-weight: 600;
    background: var(--surface2);
    border-bottom: 1px solid var(--border);
    text-align: left;
  }}
  .product-row {{
    border-bottom: 1px solid var(--border);
    cursor: pointer;
    transition: background 0.15s;
  }}
  .product-row:last-child {{ border-bottom: none; }}
  .product-row:hover {{ background: var(--surface2); }}
  td {{ padding: 14px 16px; font-size: 13px; vertical-align: middle; }}

  .product-link {{
    color: var(--text);
    text-decoration: none;
    font-weight: 600;
    display: block;
  }}
  .product-link:hover {{ color: var(--accent); }}
  .notes {{ font-size: 11px; color: var(--muted); margin-top: 3px; }}

  /* Badges */
  .badge {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.5px;
  }}
  .badge.wb {{ background: rgba(203,17,171,0.15); color: var(--wb); border: 1px solid rgba(203,17,171,0.3); }}
  .badge.oz {{ background: rgba(0,91,255,0.15); color: #6ba3ff; border: 1px solid rgba(0,91,255,0.3); }}
  .badge.ym {{ background: rgba(252,63,29,0.15); color: #ff8a70; border: 1px solid rgba(252,63,29,0.3); }}

  /* Price change */
  .change {{ font-family: 'JetBrains Mono', monospace; font-size: 12px; font-weight: 600; padding: 3px 8px; border-radius: 6px; }}
  .change.down {{ background: rgba(34,197,94,0.12); color: var(--green); }}
  .change.up {{ background: rgba(239,68,68,0.12); color: var(--red); }}
  .change.neutral {{ color: var(--muted); }}

  .td-price {{ font-family: 'JetBrains Mono', monospace; font-size: 14px; }}
  .td-threshold {{ font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--muted); }}
  .td-time {{ font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--muted); }}

  /* Empty state */
  .empty-state {{
    text-align: center;
    padding: 60px 20px;
    color: var(--muted);
  }}
  .empty-state h3 {{ font-size: 18px; margin-bottom: 8px; color: var(--text); }}

  /* Footer */
  footer {{
    text-align: center;
    padding: 24px;
    color: var(--muted);
    font-size: 12px;
    border-top: 1px solid var(--border);
  }}

  @media (max-width: 768px) {{
    header {{ padding: 16px 20px; }}
    .stats-row {{ grid-template-columns: repeat(2,1fr); padding: 16px 20px; }}
    .content {{ padding: 0 20px 32px; }}
    .td-time, .td-threshold {{ display: none; }}
  }}
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-icon">📊</div>
    <div>
      <div class="logo-text">Price Monitor</div>
      <div class="logo-sub">Мониторинг цен на маркетплейсах</div>
    </div>
  </div>
  <div class="last-update">Обновлено: <span>{last_upd}</span></div>
</header>

<div class="stats-row">
  <div class="stat-card">
    <div class="stat-label">Товаров</div>
    <div class="stat-value" style="color:var(--accent)">{stats.get('tracked_products', 0)}</div>
    <div class="stat-sub">активных в мониторинге</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">Проверок</div>
    <div class="stat-value">{stats.get('total_checks', 0)}</div>
    <div class="stat-sub">всего замеров цен</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">Алертов</div>
    <div class="stat-value" style="color:var(--green)">{stats.get('total_alerts', 0)}</div>
    <div class="stat-sub">уведомлений отправлено</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">Интервал</div>
    <div class="stat-value" style="color:var(--accent2)">3ч</div>
    <div class="stat-sub">частота обновления</div>
  </div>
</div>

<div class="content">

  <!-- Панель с графиком (скрыта по умолчанию) -->
  <div class="chart-panel" id="chartPanel">
    <div class="chart-header">
      <div class="chart-title" id="chartTitle">График цен</div>
      <button class="chart-close" onclick="hideChart()">✕</button>
    </div>
    <canvas id="priceChart"></canvas>
  </div>

  <!-- Таблица товаров -->
  <div class="table-wrap">
    <div class="table-header">
      <div class="table-title">📦 Отслеживаемые товары</div>
      <div class="table-hint">Нажмите на строку для просмотра графика</div>
    </div>
    {'<table><thead><tr><th>Товар</th><th>Магазин</th><th>Цена</th><th>Изм.</th><th>Наличие</th><th>Порог</th><th>Проверено</th></tr></thead><tbody>' + rows_html + '</tbody></table>'
      if products_data else
      '<div class="empty-state"><h3>Нет данных</h3><p>Дождитесь первого запуска мониторинга</p></div>'
    }
  </div>
</div>

<footer>
  Price Monitor · Обновляется каждые 3 часа · GitHub Actions + GitHub Pages
</footer>

<script>
const CHARTS_DATA = {charts_js};
let currentChart = null;

function showChart(productId) {{
  const data = CHARTS_DATA[productId];
  if (!data || !data.labels.length) {{
    alert('Нет данных для графика — дождитесь нескольких проверок');
    return;
  }}

  const panel = document.getElementById('chartPanel');
  const title = document.getElementById('chartTitle');
  panel.classList.add('active');

  // Находим имя товара
  const row = document.querySelector(`[onclick="showChart('${{productId}}')"]`);
  const name = row ? row.querySelector('.product-link').textContent : productId;
  title.textContent = `📈 ${{name}}`;

  if (currentChart) currentChart.destroy();

  const ctx = document.getElementById('priceChart').getContext('2d');

  // Определяем тренд для цвета
  const prices = data.prices;
  const isDown = prices.length > 1 && prices[prices.length-1] < prices[0];
  const lineColor = isDown ? '#22c55e' : '#4f8eff';
  const fillColor = isDown ? 'rgba(34,197,94,0.08)' : 'rgba(79,142,255,0.08)';

  currentChart = new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: data.labels,
      datasets: [{{
        label: 'Цена (₽)',
        data: data.prices,
        borderColor: lineColor,
        backgroundColor: fillColor,
        fill: true,
        tension: 0.4,
        pointBackgroundColor: lineColor,
        pointRadius: 4,
        pointHoverRadius: 7,
        borderWidth: 2.5,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: true,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#1c2030',
          borderColor: '#252a3a',
          borderWidth: 1,
          titleColor: '#e2e8f0',
          bodyColor: '#94a3b8',
          callbacks: {{
            label: ctx => ` ${{ctx.parsed.y.toLocaleString('ru-RU')}} ₽`
          }}
        }}
      }},
      scales: {{
        x: {{
          grid: {{ color: 'rgba(255,255,255,0.04)' }},
          ticks: {{ color: '#64748b', font: {{ family: 'JetBrains Mono', size: 11 }} }}
        }},
        y: {{
          grid: {{ color: 'rgba(255,255,255,0.04)' }},
          ticks: {{
            color: '#64748b',
            font: {{ family: 'JetBrains Mono', size: 11 }},
            callback: v => v.toLocaleString('ru-RU') + ' ₽'
          }}
        }}
      }}
    }}
  }});

  panel.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
}}

function hideChart() {{
  document.getElementById('chartPanel').classList.remove('active');
  if (currentChart) {{ currentChart.destroy(); currentChart = null; }}
}}
</script>

</body>
</html>"""

    out_path = os.path.join(DOCS_DIR, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Дашборд сохранён: {out_path}")
    return out_path


if __name__ == "__main__":
    generate_dashboard()
    print("Готово!")
