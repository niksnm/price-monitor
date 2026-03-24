"""
scraping_client.py — Ядро системы парсинга через ScraperAPI
=============================================================

ЧТО ДЕЛАЕТ ЭТОТ ФАЙЛ:
  Вместо того чтобы напрямую стучаться на Ozon/ЯМ (что блокируется),
  мы отправляем запрос на сервер ScraperAPI. Он сам:
    1. Выбирает подходящий резидентный прокси в России
    2. Рендерит JavaScript (как настоящий браузер)
    3. Решает капчу автоматически
    4. Возвращает нам готовый HTML как будто мы его открыли в Chrome

КАК ВЫГЛЯДИТ ЗАПРОС:
  Обычный запрос:  requests.get("https://www.ozon.ru/product/...")
  Через ScraperAPI: requests.get("http://api.scraperapi.com?api_key=КЛЮЧ&url=https://www.ozon.ru/product/...&render=true")

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (задаются в GitHub Secrets):
  SCRAPER_API_KEY  — API-ключ от ScraperAPI (обязательно)
  SCRAPER_PREMIUM  — "true" если куплен Premium план (необязательно)

СТОИМОСТЬ:
  Бесплатно: 1 000 запросов/месяц
  Hobby    : $49/мес → 250 000 запросов
  Расчёт   : 10 товаров × 2 сайта × 8 проверок/день × 30 дней = 4800 запросов/мес
              → Hobby план покрывает с запасом 50x
"""

import os
import time
import requests
from urllib.parse import quote
from typing import Optional, Dict, Tuple


# ─────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ ScraperAPI
# ─────────────────────────────────────────────────────────────

SCRAPERAPI_BASE = "http://api.scraperapi.com"

# Счётчик использованных запросов в текущей сессии
_requests_used = 0
_requests_failed = 0


def get_api_key() -> Optional[str]:
    """
    Возвращает API-ключ ScraperAPI из переменной окружения.
    Переменная SCRAPER_API_KEY должна быть добавлена в GitHub Secrets.
    """
    key = os.environ.get("SCRAPER_API_KEY", "").strip()
    return key if key else None


def is_premium() -> bool:
    """Возвращает True если включён Premium план (лучший обход защиты)."""
    return os.environ.get("SCRAPER_PREMIUM", "").lower() == "true"


# ─────────────────────────────────────────────────────────────
# ОСНОВНАЯ ФУНКЦИЯ ЗАПРОСА
# ─────────────────────────────────────────────────────────────

def scrape_url(
    url: str,
    render_js: bool = True,
    country_code: str = "ru",
    retry_count: int = 3,
    retry_delay: float = 5.0,
    timeout: int = 60,
    session_number: Optional[int] = None,
    ultra_premium: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Получает HTML страницы через ScraperAPI.

    ПАРАМЕТРЫ:
      url           — URL страницы которую хотим получить (Ozon, ЯМ и т.д.)
      render_js     — True = рендерить JavaScript (нужно для Ozon, ЯМ)
                      False = просто HTML без JS (быстрее, для простых сайтов)
      country_code  — код страны для прокси ("ru" = Россия, обязательно!)
      retry_count   — сколько раз повторить при ошибке (по умолчанию 3)
      retry_delay   — пауза между попытками в секундах
      timeout       — максимальное время ожидания ответа (60 сек для JS-рендера)
      session_number — номер сессии (один IP на всю сессию, для связанных запросов)
      ultra_premium — True = использовать Ultra Premium прокси (резидентные)

    ВОЗВРАЩАЕТ:
      (html_text, None)        — если успешно, html_text это HTML страницы
      (None, error_message)    — если ошибка, error_message описывает проблему

    ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:
      html, err = scrape_url("https://www.ozon.ru/product/...")
      if err:
          print(f"Ошибка: {err}")
      else:
          # Парсим html
          soup = BeautifulSoup(html, "lxml")
    """
    global _requests_used, _requests_failed

    api_key = get_api_key()
    if not api_key:
        return None, (
            "SCRAPER_API_KEY не задан! "
            "Добавьте ключ в GitHub Secrets → Settings → Secrets → Actions → "
            "New secret → Name: SCRAPER_API_KEY"
        )

    # Собираем параметры запроса к ScraperAPI
    params = {
        "api_key": api_key,
        "url": url,
        "country_code": country_code,
    }

    # JavaScript рендеринг (нужен для Ozon и Яндекс.Маркет)
    if render_js:
        params["render"] = "true"

    # Premium прокси — лучше обходят защиту, расходуют 10 кредитов вместо 1
    if is_premium() or ultra_premium:
        params["premium"] = "true"

    # Сессионный IP — для одного товара используется один IP
    if session_number is not None:
        params["session_number"] = str(session_number)

    # Попытки запроса с повторами при ошибках
    last_error = None

    for attempt in range(1, retry_count + 1):

        if attempt > 1:
            wait = retry_delay * attempt  # Каждый раз ждём дольше
            print(f"     ⏳ Попытка {attempt}/{retry_count} через {wait:.0f}с...")
            time.sleep(wait)

        try:
            print(f"     🌐 ScraperAPI запрос (попытка {attempt}/{retry_count}): {url[:60]}...")

            response = requests.get(
                SCRAPERAPI_BASE,
                params=params,
                timeout=timeout,
            )

            _requests_used += 1

            # Анализируем ответ
            if response.status_code == 200:
                html = response.text

                # Проверяем что получили реальную страницу, а не заглушку
                if len(html) < 1000:
                    last_error = f"Слишком короткий ответ ({len(html)} символов) — вероятно капча"
                    _requests_failed += 1
                    continue

                # Признаки что нас всё равно заблокировали
                block_signals = [
                    "captcha", "recaptcha", "cf-challenge",
                    "access denied", "403 forbidden",
                    "robot", "blocked", "cloudflare"
                ]
                html_lower = html.lower()
                for signal in block_signals:
                    if signal in html_lower and len(html) < 50000:
                        last_error = f"Обнаружена защита ('{signal}') в ответе"
                        _requests_failed += 1
                        # Пробуем с premium на следующей попытке
                        params["premium"] = "true"
                        break
                else:
                    # Всё хорошо — возвращаем HTML
                    print(f"     ✅ Получено {len(html):,} символов HTML")
                    return html, None

            elif response.status_code == 401:
                # Неверный API-ключ
                return None, (
                    "Неверный SCRAPER_API_KEY (HTTP 401). "
                    "Проверьте ключ в GitHub Secrets."
                )

            elif response.status_code == 403:
                # Лимит исчерпан или аккаунт заблокирован
                return None, (
                    "ScraperAPI вернул 403 — возможно исчерпан лимит запросов. "
                    "Проверьте дашборд: https://dashboard.scraperapi.com"
                )

            elif response.status_code == 500:
                last_error = f"ScraperAPI внутренняя ошибка (500) — попробуем снова"
                _requests_failed += 1

            elif response.status_code == 408:
                last_error = "Таймаут рендеринга (408) — страница грузилась слишком долго"
                _requests_failed += 1

            else:
                last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                _requests_failed += 1

        except requests.exceptions.Timeout:
            last_error = f"Таймаут {timeout}с — ScraperAPI не ответил"
            _requests_failed += 1

        except requests.exceptions.ConnectionError as e:
            last_error = f"Ошибка соединения с ScraperAPI: {e}"
            _requests_failed += 1

        except Exception as e:
            last_error = f"Неожиданная ошибка: {type(e).__name__}: {e}"
            _requests_failed += 1

    # Все попытки исчерпаны
    return None, f"Все {retry_count} попытки неудачны. Последняя ошибка: {last_error}"


# ─────────────────────────────────────────────────────────────
# ПРОВЕРКА ОСТАТКА ЗАПРОСОВ
# ─────────────────────────────────────────────────────────────

def check_account_status() -> Dict:
    """
    Проверяет статус аккаунта ScraperAPI:
    сколько запросов осталось в этом месяце.

    Используется при запуске мониторинга чтобы убедиться
    что API-ключ работает и запросы ещё есть.

    ВОЗВРАЩАЕТ словарь:
      {
        "ok": True/False,
        "requests_left": 800,
        "requests_limit": 1000,
        "plan": "free",
        "error": None или текст ошибки
      }
    """
    api_key = get_api_key()
    if not api_key:
        return {"ok": False, "error": "API-ключ не задан", "requests_left": 0}

    try:
        # Специальный эндпоинт ScraperAPI для проверки аккаунта
        resp = requests.get(
            f"http://api.scraperapi.com/account",
            params={"api_key": api_key},
            timeout=15
        )

        if resp.status_code == 200:
            data = resp.json()
            # requestCount — использовано, requestLimit — лимит
            used = data.get("requestCount", 0)
            limit = data.get("requestLimit", 1000)
            left = limit - used

            return {
                "ok": True,
                "requests_used": used,
                "requests_left": left,
                "requests_limit": limit,
                "plan": "premium" if limit > 10000 else "free",
                "error": None
            }
        else:
            return {
                "ok": False,
                "error": f"HTTP {resp.status_code}: Неверный ключ или проблема с аккаунтом",
                "requests_left": 0
            }

    except Exception as e:
        return {"ok": False, "error": str(e), "requests_left": 0}


# ─────────────────────────────────────────────────────────────
# СТАТИСТИКА ТЕКУЩЕЙ СЕССИИ
# ─────────────────────────────────────────────────────────────

def print_session_stats():
    """Выводит статистику использования API за текущий запуск."""
    print(f"\n📊 ScraperAPI статистика сессии:")
    print(f"   Запросов использовано: {_requests_used}")
    print(f"   Неудачных: {_requests_failed}")
    success_rate = 0
    if _requests_used > 0:
        success_rate = ((_requests_used - _requests_failed) / _requests_used) * 100
    print(f"   Успешность: {success_rate:.0f}%")


# ─────────────────────────────────────────────────────────────
# ТЕСТ ПРИ ПРЯМОМ ЗАПУСКЕ
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("🧪 Тест ScraperAPI клиента")
    print("=" * 60)

    # Проверяем аккаунт
    print("\n1. Проверка аккаунта:")
    status = check_account_status()
    if status["ok"]:
        print(f"   ✅ Аккаунт активен")
        print(f"   📊 Запросов использовано: {status['requests_used']}")
        print(f"   📊 Запросов осталось: {status['requests_left']} из {status['requests_limit']}")
    else:
        print(f"   ❌ Ошибка: {status['error']}")
        print("   Установите SCRAPER_API_KEY в переменных окружения")
        exit(1)

    # Тест на простом сайте
    print("\n2. Тест запроса (httpbin.org — простой тест без JS):")
    html, err = scrape_url("https://httpbin.org/ip", render_js=False, country_code="ru")
    if html:
        print(f"   ✅ Ответ получен: {html.strip()[:200]}")
    else:
        print(f"   ❌ Ошибка: {err}")

    # Тест на Ozon
    print("\n3. Тест на Ozon (с JS-рендерингом, это может занять 30-60 сек):")
    html, err = scrape_url(
        "https://www.ozon.ru/product/smartfon-xiaomi-redmi-note-13-1291826178/",
        render_js=True,
        country_code="ru",
        retry_count=2
    )
    if html:
        print(f"   ✅ Ozon ответил: {len(html):,} символов HTML")
    else:
        print(f"   ❌ Ошибка Ozon: {err}")

    print_session_stats()
    print("\n" + "=" * 60)
