# courier_mules_project/scripts/publisher_logic.py

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

# Добавляем путь к текущей директории для импорта config
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(script_dir)

try:
    # Пытаемся импортировать из scripts/publisher_config.py
    from publisher_config import PUBLISH_CONFIG, CITIES
    logger_ready = True
except ImportError as e:
    # Если нет publisher_config.py, используем fallback конфиг
    logger_ready = False
    # Сначала настроим минимальный логгер для вывода ошибки
    logging.basicConfig(level=logging.ERROR)
    temp_logger = logging.getLogger(__name__)
    temp_logger.error(f"Не удалось импортировать publisher_config: {e}")
    temp_logger.info("Используется fallback конфигурация...")

# Теперь настраиваем полноценное логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Fallback конфиг если не удалось импортировать
if not logger_ready:
    PUBLISH_CONFIG = {
        "criteria": {
            "max_vacancy_age_days": 30,
            "max_parsed_age_days": 7,
            "currency": "RUR",
        },
        "publication": {
            "vacancies_per_post": 5,
            "post_times_msk": ["09:00", "13:00", "19:00", "21:00"],
        },
        "formatting": {
            "emojis": {
                "title": "🚴",
                "salary": "💰",
                "company": "🏢",
                "date": "📅",
                "payment": "💳",
                "employer": "✅",
                "divider": "---",
            },
            "referral_link": "https://ya.cc/8UiUqj",
        }
    }
    
    CITIES = {
        "msk": {"channel": "@courier_jobs_msk", "name": "Москва"},
        "spb": {"channel": "@courier_jobs_spb", "name": "Санкт-Петербург"},
        "nsk": {"channel": "@courier_jobs_nsk", "name": "Новосибирск"},
        "ekb": {"channel": "@courier_jobs_ekb", "name": "Екатеринбург"},
        "kzn": {"channel": "@courier_jobs_kzn", "name": "Казань"},
    }
    logger.info("Используется fallback конфигурация")

# Импортируем supabase после настройки логирования
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError as e:
    logger.error(f"Не удалось импортировать supabase: {e}")
    SUPABASE_AVAILABLE = False
    # Создаем заглушку для типа
    from typing import Any
    Client = Any


def should_publish_now() -> bool:
    """
    Проверяет, нужно ли публиковать сейчас по московскому времени.
    
    В GitHub Actions всегда возвращает True, так как триггер уже настроен.
    Для локального тестирования можно использовать проверку времени.
    """
    # Для GitHub Actions всегда публикуем
    if "GITHUB_ACTIONS" in os.environ:
        logger.info("📱 Режим GitHub Actions - публикация разрешена")
        return True
    
    # Для локального тестирования проверяем время
    try:
        post_times = PUBLISH_CONFIG["publication"]["post_times_msk"]
        now_msk = datetime.now(timezone(timedelta(hours=3)))
        current_time = now_msk.strftime("%H:%M")
        
        logger.info(f"⏰ Текущее время (Мск): {current_time}")
        logger.info(f"⏰ Время публикации по расписанию: {', '.join(post_times)}")
        
        for scheduled_time in post_times:
            try:
                scheduled_hour, scheduled_minute = map(int, scheduled_time.split(":"))
                scheduled_dt = now_msk.replace(hour=scheduled_hour, minute=scheduled_minute, second=0)
                
                time_diff = abs((now_msk - scheduled_dt).total_seconds() / 60)
                
                if time_diff <= 10:  # +-10 минут
                    logger.info(f"✅ Время для публикации! (+/-10 минут от {scheduled_time})")
                    return True
            except (ValueError, AttributeError):
                continue
        
        logger.info("⏸️  Не время для публикации по расписанию")
        return False
        
    except Exception as e:
        logger.warning(f"⚠️  Ошибка проверки времени: {e}")
        # В случае ошибки - разрешаем публикацию
        return True


def get_vacancies_for_publication(
    supabase_client: Client,
    city_slug: str,
    limit: int = 5
) -> List[Dict]:
    """
    Получает вакансии для публикации.
    
    Критерии из publisher_config.py:
    1. is_posted = FALSE
    2. published_at не старше max_vacancy_age_days (30 дней)
    3. created_at не старше max_parsed_age_days (7 дней)
    4. currency = 'RUR'
    """
    
    # Рассчитываем даты-ограничители
    now = datetime.now(timezone.utc)
    max_vacancy_date = now - timedelta(days=PUBLISH_CONFIG["criteria"]["max_vacancy_age_days"])
    max_parsed_date = now - timedelta(days=PUBLISH_CONFIG["criteria"]["max_parsed_age_days"])
    
    logger.info(f"Критерии отбора для {city_slug}:")
    logger.info(f"  - published_at >= {max_vacancy_date.strftime('%Y-%m-%d')}")
    logger.info(f"  - created_at >= {max_parsed_date.strftime('%Y-%m-%d')}")
    logger.info(f"  - currency = 'RUR'")
    logger.info(f"  - is_posted = FALSE")
    
    # Строим запрос
    query = (
        supabase_client
        .table("vacancies")
        .select("*")
        .eq("city_slug", city_slug)
        .eq("is_posted", False)
        .eq("currency", PUBLISH_CONFIG["criteria"]["currency"])
        .gte("published_at", max_vacancy_date.isoformat())
        .gte("created_at", max_parsed_date.isoformat())
    )
    
    # ВАЖНО: Для версии supabase 1.1.1 параметр nulls_last не поддерживается
    # Используем альтернативный подход
    query = query.order("salary_to_net", desc=True)
    query = query.order("published_at", desc=True)
    
    # Лимит
    query = query.limit(limit)
    
    # Выполняем
    try:
        response = query.execute()
        logger.info(f"Найдено {len(response.data)} вакансий для {city_slug}")
        
        # Вручную сортируем, чтобы вакансии без зарплаты были в конце
        if response.data:
            # Разделяем на вакансии с зарплатой и без
            with_salary = []
            without_salary = []
            
            for vacancy in response.data:
                if vacancy.get("salary_to_net") is not None:
                    with_salary.append(vacancy)
                else:
                    without_salary.append(vacancy)
            
            # Сортируем вакансии с зарплатой по убыванию
            with_salary.sort(key=lambda x: x.get("salary_to_net", 0), reverse=True)
            
            # Объединяем: сначала с зарплатой, потом без
            sorted_vacancies = with_salary + without_salary
            
            # Ограничиваем лимитом
            return sorted_vacancies[:limit]
        
        return response.data if response.data else []
        
    except Exception as e:
        logger.error(f"Ошибка при запросе вакансий для {city_slug}: {str(e)}")
        return []


def format_salary_display(vacancy: Dict) -> str:
    """Форматирует отображение зарплаты."""
    parts = []
    
    # Зарплата "от" (на руки)
    if vacancy.get("salary_from_net"):
        parts.append(f"от {vacancy['salary_from_net']:,} ₽".replace(",", " "))
    
    # Зарплата "до" (на руки) - приоритетная
    if vacancy.get("salary_to_net"):
        if vacancy.get("salary_from_net"):
            parts.append(f"до {vacancy['salary_to_net']:,} ₽".replace(",", " "))
        else:
            parts.append(f"{vacancy['salary_to_net']:,} ₽".replace(",", " "))
    
    return " ".join(parts) if parts else ""


def format_payment_info(vacancy: Dict) -> str:
    """Форматирует информацию о выплатах."""
    parts = []
    
    # Форма выплат (за месяц/день и т.д.)
    if vacancy.get("salary_period_name"):
        parts.append(vacancy["salary_period_name"])
    
    # Частота выплат
    if vacancy.get("salary_frequency_name"):
        parts.append(f"({vacancy['salary_frequency_name']})")
    
    return " ".join(parts) if parts else ""


def format_publication_date(published_at: str) -> str:
    """Форматирует дату публикации."""
    try:
        pub_date = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff = now - pub_date
        
        if diff.days == 0:
            return "сегодня"
        elif diff.days == 1:
            return "вчера"
        elif diff.days < 7:
            return f"{diff.days} дня назад"
        elif diff.days < 30:
            weeks = diff.days // 7
            return f"{weeks} недел{'ю' if weeks == 1 else 'и' if weeks < 5 else 'ь'} назад"
        else:
            return pub_date.strftime("%d.%m.%Y")
    except:
        return ""


def format_post_with_vacancies(vacancies: List[Dict], city_name: str) -> Tuple[str, Optional[str]]:
    """
    Форматирует пост с несколькими вакансиями.
    
    Возвращает:
    - Текст поста
    - Ссылка для кнопки (если есть реферальная ссылка)
    """
    if not vacancies:
        return "Нет новых вакансий для публикации", None
    
    emojis = PUBLISH_CONFIG["formatting"]["emojis"]
    
    # Исправляем заголовок: "в г. Казань" вместо "в Казань"
    header = f"<b>🚀 Новые вакансии курьеров в г. {city_name}</b>\n\n"
    
    # Форматируем вакансии
    vacancy_sections = []
    for i, vacancy in enumerate(vacancies, 1):
        vacancy_text = f"<b>{i}. {vacancy['title']} в {vacancy['employer']}</b>\n"
        
        # Зарплата
        salary_display = format_salary_display(vacancy)
        if salary_display:
            vacancy_text += f"{emojis['salary']} <b>{salary_display}</b>\n"
        
        # График
        if vacancy.get('schedule_name'):
            vacancy_text += f"{emojis['schedule']} {vacancy['schedule_name']}\n"
        
        # Опыт
        if vacancy.get('experience_name'):
            vacancy_text += f"{emojis['experience']} {vacancy['experience_name']}\n"
        
        # Ссылка
        vacancy_text += f"📌 <a href='{vacancy['external_url']}'>Подробнее на HH.ru</a>\n"
        
        if i < len(vacancies):
            vacancy_text += f"\n{emojis['divider']}\n\n"
        
        vacancy_sections.append(vacancy_text)
    
    # Собираем пост
    post_text = header + "".join(vacancy_sections)
    
    # Улучшенный CTA для реферальной ссылки
    footer = f"\n\n💡 <b>Хочешь работать на себя?</b>\n\n"
    footer += "✅ Работай на себя — сам выбираешь график\n"
    footer += "✅ Заработок от 1500₽ в день с первого дня\n"
    footer += "✅ Выплаты ежедневно на карту\n"
    footer += "✅ Работаешь в своём районе — без долгих поездок\n"
    footer += "✅ Бонусы для новичков\n\n"
    
    # Реферальная ссылка с сильным CTA
    referral_link = PUBLISH_CONFIG["formatting"].get("referral_link")
    if referral_link:
        footer += f"🚀 <b><a href='{referral_link}'>Начать работать на себя →</a></b>\n"
        footer += f"<i>Начни зарабатывать уже завтра!</i>"
    
    post_text += footer
    
    # Проверяем длину сообщения (Telegram ограничение: 4096 символов)
    if len(post_text) > 4096:
        logger.warning(f"Сообщение слишком длинное ({len(post_text)} символов), обрезаем...")
        # Оставляем только первые 3 вакансии
        return format_post_with_vacancies(vacancies[:3], city_name)
    
    return post_text, referral_link


def mark_vacancies_as_posted(
    supabase_client: Client,
    vacancy_ids: List[int],
    channel_id: str
) -> bool:
    """
    Помечает вакансии как опубликованные.
    
    Возвращает True если успешно, False если ошибка.
    """
    if not vacancy_ids:
        return True
    
    try:
        now = datetime.now(timezone.utc).isoformat()
        
        # Обновляем все вакансии одним запросом
        response = (
            supabase_client
            .table("vacancies")
            .update({
                "is_posted": True,
                "posted_at": now,
                "channel_id": channel_id,
                "updated_at": now
            })
            .in_("id", vacancy_ids)
            .execute()
        )
        
        logger.info(f"Отмечено {len(vacancy_ids)} вакансий как опубликованные")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении вакансий: {str(e)}")
        return False


def publish_to_telegram(
    bot_token: str,
    channel_id: str,
    post_text: str,
    button_url: Optional[str] = None
) -> bool:
    """
    Публикует пост в Telegram канал.
    
    Возвращает True если успешно, False если ошибка.
    """
    try:
        import requests
        
        # Создаем кнопку с улучшенным текстом
        reply_markup = None
        if button_url:
            reply_markup = {
                "inline_keyboard": [[
                    {"text": "🚀 Работать на себя", "url": button_url}
                ]]
            }
        
        # Отправляем сообщение
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": channel_id,
            "text": post_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,  # Разрешаем превью для реферальной ссылки
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        logger.info(f"Пост успешно опубликован в {channel_id}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка публикации в Telegram: {str(e)}")
        return False


def publish_city_vacancies(
    supabase_client: Client,
    bot_token: str,
    city_slug: str
) -> Tuple[bool, str, int]:
    """
    Основная функция публикации вакансий для города.
    
    Возвращает:
    - Успех/неудача (bool)
    - Сообщение о результате (str)
    - Количество опубликованных вакансий (int)
    """
    try:
        # Получаем данные города
        city_info = CITIES.get(city_slug)
        if not city_info:
            return False, f"Город {city_slug} не найден в конфигурации", 0
        
        vacancies_per_post = PUBLISH_CONFIG["publication"]["vacancies_per_post"]
        
        logger.info(f"Ищу до {vacancies_per_post} вакансий для {city_info['name']}...")
        
        # Получаем вакансии для публикации
        vacancies = get_vacancies_for_publication(
            supabase_client, 
            city_slug, 
            limit=vacancies_per_post
        )
        
        if not vacancies:
            logger.info(f"Нет новых вакансий для публикации в {city_info['name']}")
            return True, f"Нет новых вакансий для {city_info['name']}", 0
        
        logger.info(f"Найдено {len(vacancies)} вакансий для публикации в {city_info['name']}")
        
        # Форматируем пост
        post_text, button_url = format_post_with_vacancies(
            vacancies, 
            city_info['name']
        )
        
        logger.info(f"Публикую в Telegram канал: {city_info['channel']}")
        
        # Публикуем в Telegram
        success = publish_to_telegram(
            bot_token,
            city_info['channel'],
            post_text,
            button_url
        )
        
        if not success:
            logger.error(f"Не удалось опубликовать в {city_info['channel']}")
            return False, f"Ошибка публикации в {city_info['name']}", 0
        
        # Помечаем вакансии как опубликованные
        vacancy_ids = [v['id'] for v in vacancies]
        mark_success = mark_vacancies_as_posted(
            supabase_client,
            vacancy_ids,
            city_info['channel']
        )
        
        if not mark_success:
            logger.warning(f"Вакансии опубликованы, но не отмечены в БД для {city_info['name']}")
        
        return True, f"Опубликовано {len(vacancies)} вакансий", len(vacancies)
        
    except Exception as e:
        logger.error(f"Критическая ошибка при публикации {city_slug}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False, f"Критическая ошибка: {str(e)}", 0


def main_publisher() -> Tuple[bool, Dict[str, int]]:
    """
    Основная функция публикации для всех городов.
    
    Возвращает:
    - Общий успех (bool) - True если все города обработаны успешно
    - Статистика по городам {city_slug: количество_вакансий}
    """
    # Получаем конфигурацию из переменных окружения
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    bot_token = os.environ.get("TG_BOT_TOKEN")
    
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ПУБЛИКАТОРА ВАКАНСИЙ")
    logger.info(f"   Дата: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"   Режим: {'GitHub Actions' if 'GITHUB_ACTIONS' in os.environ else 'Локальный'}")
    logger.info(f"   Триггер: {os.environ.get('GITHUB_EVENT_NAME', 'Неизвестно')}")
    logger.info("=" * 60)
    
    # Подробное логирование переменных окружения
    logger.info("🔧 ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ:")
    logger.info(f"  SUPABASE_URL: {'✅ УСТАНОВЛЕНА' if supabase_url else '❌ ОТСУТСТВУЕТ'}")
    logger.info(f"  SUPABASE_KEY: {'✅ УСТАНОВЛЕНА' if supabase_key else '❌ ОТСУТСТВУЕТ'}")
    logger.info(f"  TG_BOT_TOKEN: {'✅ УСТАНОВЛЕНА' if bot_token else '❌ ОТСУТСТВУЕТ'}")
    
    if not supabase_url or not supabase_key or not bot_token:
        logger.error("❌ ОШИБКА: Не все обязательные переменные установлены")
        return False, {}
    
    # Проверяем, нужно ли публиковать (только для локального запуска)
    if not should_publish_now():
        logger.info("⏸️  Не время для публикации по расписанию")
        return True, {}
    
    # Проверяем доступность supabase
    if not SUPABASE_AVAILABLE:
        logger.error("❌ ОШИБКА: Библиотека supabase не установлена")
        return False, {}
    
    # Подключаемся к Supabase
    try:
        logger.info("\n🔗 Подключаюсь к Supabase...")
        supabase_client = create_client(supabase_url, supabase_key)
        
        # Делаем тестовый запрос для проверки подключения
        test_result = supabase_client.table("vacancies").select("id", count="exact").limit(1).execute()
        logger.info(f"✅ Успешное подключение к Supabase")
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Supabase: {str(e)}")
        return False, {}
    
    # Публикуем для каждого города
    logger.info(f"\n📍 ПУБЛИКАЦИЯ ДЛЯ {len(CITIES)} ГОРОДОВ")
    
    results = {}
    all_success = True
    total_vacancies = 0
    
    for city_slug in CITIES.keys():
        city_name = CITIES[city_slug]["name"]
        logger.info(f"\n{'='*50}")
        logger.info(f"📍 ГОРОД: {city_name.upper()} ({city_slug})")
        logger.info(f"   Канал: {CITIES[city_slug]['channel']}")
        
        success, message, count = publish_city_vacancies(
            supabase_client,
            bot_token,
            city_slug
        )
        
        results[city_slug] = count
        total_vacancies += count
        
        if success:
            if count > 0:
                logger.info(f"✅ {message}")
            else:
                logger.info(f"ℹ️  {message}")
        else:
            logger.error(f"❌ {message}")
            all_success = False
        
        # Небольшая задержка между городами
        import time
        time.sleep(1)
    
    # Итоговая статистика
    logger.info(f"\n{'='*60}")
    logger.info("📊 ИТОГИ ПУБЛИКАЦИИ:")
    logger.info(f"{'='*60}")
    
    for city_slug, count in results.items():
        city_name = CITIES[city_slug]["name"]
        channel = CITIES[city_slug]["channel"]
        status = "✅" if count > 0 else "ℹ️ "
        logger.info(f"{status} {city_name:20} | {count:2} вакансий | {channel}")
    
    logger.info(f"{'─'*60}")
    logger.info(f"📈 Всего опубликовано: {total_vacancies} вакансий")
    
    if total_vacancies == 0:
        logger.info("ℹ️  Новых вакансий для публикации не найдено")
    
    logger.info(f"{'='*60}")
    
    return all_success, results


if __name__ == "__main__":
    """
    Точка входа для запуска из командной строки.
    """
    import sys
    
    # Запускаем публикацию
    success, stats = main_publisher()
    
    # Возвращаем код выхода для GitHub Actions
    if success:
        logger.info("✅ Публикация завершена успешно")
        sys.exit(0)
    else:
        logger.error("❌ Публикация завершена с ошибками")
        sys.exit(1)
