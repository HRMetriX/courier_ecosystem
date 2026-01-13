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
            "max_parsed_age_days": 14,
            "currency": "RUR",
        },
        "publication": {
            "vacancies_per_post": 10,
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
            "referral_link": "https://ya.cc/8UiUqj  ",
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
    target_count: int = 10,  # Целевое количество вакансий
    max_per_company: int = 2  # Максимум вакансий от одной компании
) -> List[Dict]:
    """
    Получает вакансии для публикации.
    
    Цель: получить target_count вакансий, но не более 
    max_per_company от одной компании.
    
    Алгоритм:
    1. Берем в 3 раза больше вакансий для фильтрации
    2. Идем по списку от самых высокооплачиваемых
    3. Берем максимум max_per_company от каждой компании
    4. Останавливаемся когда набрали target_count или закончились вакансии
    """
    
    # Рассчитываем даты-ограничители
    now = datetime.now(timezone.utc)
    max_vacancy_date = now - timedelta(days=PUBLISH_CONFIG["criteria"]["max_vacancy_age_days"])
    max_parsed_date = now - timedelta(days=PUBLISH_CONFIG["criteria"]["max_parsed_age_days"])
    
    logger.info(f"Критерии отбора для {city_slug}:")
    logger.info(f"  - Целевое количество: {target_count} вакансий")
    logger.info(f"  - Не более {max_per_company} от одной компании")
    logger.info(f"  - published_at >= {max_vacancy_date.strftime('%Y-%m-%d')}")
    logger.info(f"  - created_at >= {max_parsed_date.strftime('%Y-%m-%d')}")
    logger.info(f"  - currency = 'RUR'")
    logger.info(f"  - is_posted = FALSE")
    
    # Берем больше вакансий для фильтрации
    initial_limit = target_count * 3  # Берем в 3 раза больше
    
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
    
    # Сортировка: сначала вакансии с зарплатой (от высокой к низкой),
    # потом без зарплаты, все по свежести
    query = query.order("salary_to_net", desc=True)
    query = query.order("published_at", desc=True)
    
    # Лимит увеличенный
    query = query.limit(initial_limit)
    
    # Выполняем
    try:
        response = query.execute()
        logger.info(f"Найдено {len(response.data)} доступных вакансий для {city_slug}")
        
        if not response.data:
            logger.warning(f"Нет доступных вакансий для {city_slug}")
            return []
        
        # ПАСС 1: Собираем лучшие вакансии с ограничением по компаниям
        filtered_vacancies = []
        company_counter = {}
        seen_titles = set()  # Для грубой проверки дубликатов
        
        for vacancy in response.data:
            # Проверяем данные вакансии
            employer = str(vacancy.get('employer', '')).strip()
            title = str(vacancy.get('title', '')).strip()
            
            if not employer or not title:
                logger.debug(f"Пропускаем вакансию без компании или названия: {vacancy.get('id')}")
                continue
            
            # Грубая проверка на дубликаты (первые 60 символов названия)
            title_key = f"{employer}_{title[:60]}"
            if title_key in seen_titles:
                logger.debug(f"Пропускаем возможный дубликат: {title[:40]}...")
                continue
            
            # Проверяем лимит по компании
            current_count = company_counter.get(employer, 0)
            if current_count >= max_per_company:
                logger.debug(f"Лимит для {employer} исчерпан ({current_count}/{max_per_company})")
                continue
            
            # Добавляем вакансию
            seen_titles.add(title_key)
            company_counter[employer] = current_count + 1
            filtered_vacancies.append(vacancy)
            
            logger.debug(f"Добавлена вакансия: {employer} - {title[:40]}... (всего: {len(filtered_vacancies)})")
            
            # Если набрали нужное количество - останавливаемся
            if len(filtered_vacancies) >= target_count:
                logger.info(f"Набрали целевое количество вакансий: {len(filtered_vacancies)}")
                break
        
        # ПАСС 2: Если не набрали target_count, снимаем ограничение по компаниям
        if len(filtered_vacancies) < target_count and len(filtered_vacancies) < len(response.data):
            logger.warning(f"Не удалось набрать {target_count} вакансий с ограничением по компаниям")
            logger.warning(f"Набрано только {len(filtered_vacancies)}. Добавляем дополнительные...")
            
            # Проходим по оставшимся вакансиям
            for vacancy in response.data[len(filtered_vacancies):]:
                employer = str(vacancy.get('employer', '')).strip()
                title = str(vacancy.get('title', '')).strip()
                
                if not employer or not title:
                    continue
                
                # Проверяем, не добавляли ли уже эту вакансию
                title_key = f"{employer}_{title[:60]}"
                if title_key in seen_titles:
                    continue
                
                # Добавляем без ограничений по компании
                seen_titles.add(title_key)
                filtered_vacancies.append(vacancy)
                
                if len(filtered_vacancies) >= target_count:
                    logger.info(f"Добрали до {len(filtered_vacancies)} вакансий")
                    break
        
        # Итоговая статистика
        unique_companies = len(set(v.get('employer', '').strip() for v in filtered_vacancies if v.get('employer')))
        
        logger.info(f"ИТОГ для {city_slug}:")
        logger.info(f"  - Всего вакансий для публикации: {len(filtered_vacancies)}")
        logger.info(f"  - Уникальных компаний: {unique_companies}")
        logger.info(f"  - Распределение по компаниям: {company_counter}")
        
        # Логируем компании и количество их вакансий
        if company_counter:
            logger.info("  - Детали по компаниям:")
            for company, count in sorted(company_counter.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"    • {company}: {count} вакансий")
        
        return filtered_vacancies
        
    except Exception as e:
        logger.error(f"Ошибка при запросе вакансий для {city_slug}: {str(e)}")
        return []

def format_salary_display(vacancy: Dict) -> str:
    """Форматирует отображение зарплаты с учётом частоты выплат."""
    salary_display = ""
    
    # Получаем зарплату "от" и "до" (на руки)
    salary_from = vacancy.get("salary_from_net")
    salary_to = vacancy.get("salary_to_net")
    
    # Форматируем зарплату
    if salary_from and salary_to:
        if salary_from == salary_to:
            salary_display = f"{salary_from:,} ₽".replace(",", " ")
        else:
            salary_display = f"от {salary_from:,} до {salary_to:,} ₽".replace(",", " ")
    elif salary_from:
        salary_display = f"от {salary_from:,} ₽".replace(",", " ")
    elif salary_to:
        salary_display = f"{salary_to:,} ₽".replace(",", " ")
    else:
        # Если зарплаты нет вообще
        return "не указана"
    
    # Добавляем информацию о выплатах (период)
    period_name = vacancy.get("salary_period_name")
    if period_name:
        salary_display += f" ({period_name}"
        
        # Добавляем частоту, если есть
        frequency_name = vacancy.get("salary_frequency_name")
        if frequency_name and frequency_name.lower() != "не указано":
            salary_display += f", {frequency_name}"
        elif frequency_name and frequency_name.lower() == "не указано":
            # Если явно "не указано", не добавляем
            pass
        else:
            # Если frequency_name есть, но мы хотим показать "не указано"
            salary_display += ", не указано"
        
        salary_display += ")"
    
    return salary_display

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
    """
    if not vacancies:
        return "Нет новых вакансий для публикации", None
    
    emojis = PUBLISH_CONFIG["formatting"]["emojis"]
    
    # Заголовок поста
    header = f"<b>🚀 Новые вакансии курьеров в г. {city_name}</b>\n\n"
    
    # Форматируем вакансии
    vacancy_sections = []
    for i, vacancy in enumerate(vacancies, 1):
        # Ссылка в названии вакансии
        vacancy_title = vacancy['title']
        external_url = vacancy['external_url']
        
        # Форматируем строку - ВАЖНО: ссылка в названии!
        vacancy_text = f"<b>{i}. <a href='{external_url}'>{vacancy_title}</a></b>\n\n"
        
        # Компания отдельной строкой
        employer = vacancy.get('employer')
        if employer and employer.strip():
            vacancy_text += f"{emojis.get('company', '🏢')} {employer}\n"
        
        # Зарплата с частотой выплат
        salary_display = format_salary_display(vacancy)
        vacancy_text += f"{emojis.get('salary', '💰')} {salary_display}\n"
        
        # График
        schedule = vacancy.get('schedule_name')
        if schedule and schedule.strip():
            vacancy_text += f"{emojis.get('schedule', '🕒')} {schedule}\n"
        
        # Опыт
        experience = vacancy.get('experience_name')
        if experience and experience.strip():
            vacancy_text += f"{emojis.get('experience', '📊')} {experience}\n"
        
        # Разделитель между вакансиями (кроме последней)
        if i < len(vacancies):
            vacancy_text += f"\n{emojis.get('divider', '---')}\n"
        
        vacancy_sections.append(vacancy_text)
    
    # Собираем пост: заголовок → CTA → вакансии
    post_text = header
    
    # Добавляем CTA сразу после заголовка
    referral_link = PUBLISH_CONFIG["formatting"].get("referral_link")
    if referral_link:
        cta = f"======\n💡 <b>Хочешь работать на себя?</b>\n\n"
        cta += "✅ Работай на себя — сам выбираешь график\n"
        cta += "✅ Заработок от 5000₽ в день с первого дня\n"
        cta += "✅ Выплаты ежедневно на карту\n"
        cta += "✅ Работаешь в своём районе — без долгих поездок\n"
        cta += "✅ Бонусы для новичков\n\n"
        cta += f"🚀 <a href='{referral_link}'><b>Начать работать на себя →</b></a>\n"
        cta += f"<i>Начни зарабатывать уже завтра!</i>\n"
        cta += f"\n<b>Или посмотри подборку свежайших вакансий</b>\n======\n\n"
        post_text += cta

    # Добавляем вакансии
    post_text += "".join(vacancy_sections)
    
    # Проверяем длину сообщения
    if len(post_text) > 4096:
        logger.warning(f"Сообщение слишком длинное ({len(post_text)} символов), обрезаем...")
        return format_post_with_vacancies(vacancies[:5], city_name)  # Оставляем только 5 вакансий
    
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
        logger.info("Нет вакансий для обновления")
        return True
    
    try:
        now = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"🔧 Обновляю {len(vacancy_ids)} вакансий:")
        logger.info(f"   is_posted: False → True")
        logger.info(f"   posted_at: NULL → {now[:19]}")
        logger.info(f"   channel_id: NULL → {channel_id}")
        
        # Обновляем все вакансии одним запросом
        response = (
            supabase_client
            .table("vacancies")
            .update({
                "is_posted": True,  # Важно: boolean True
                "posted_at": now,
                "channel_id": channel_id,
                "updated_at": now
            })
            .in_("id", vacancy_ids)
            .execute()
        )
        
        # Проверяем результат
        if hasattr(response, 'data'):
            logger.info(f"✅ Успешно обновлено {len(response.data)} записей")
        else:
            logger.info("✅ Запрос выполнен, проверяем вручную")
        
        # Дополнительная проверка
        check_query = (
            supabase_client
            .table("vacancies")
            .select("id, is_posted, posted_at")
            .in_("id", vacancy_ids[:3])  # Проверим первые 3
            .execute()
        )
        
        for vac in check_query.data:
            status = "✅" if vac.get('is_posted') else "❌"
            logger.info(f"   {status} Вакансия {vac['id']}: is_posted={vac.get('is_posted')}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении вакансий: {str(e)}")
        logger.error("   Проверьте:")
        logger.error("   1. Тип поля is_posted в Supabase")
        logger.error("   2. Права доступа к таблице")
        logger.error("   3. Корректность vacancy_ids")
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
        
        # Получаем настройки из конфига
        target_count = PUBLISH_CONFIG["publication"]["vacancies_per_post"]
        max_per_company = PUBLISH_CONFIG.get("filters", {}).get("max_vacancies_per_company", 2)
        
        logger.info(f"Ищу до {target_count} вакансий для {city_info['name']}...")
        logger.info(f"Максимум {max_per_company} вакансий от одной компании")
        
        # Получаем вакансии для публикации
        vacancies = get_vacancies_for_publication(
            supabase_client, 
            city_slug, 
            target_count=target_count,
            max_per_company=max_per_company
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
        logger.info(f"Длина поста: {len(post_text)} символов")
        
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
        else:
            logger.info(f"Успешно отмечено {len(vacancy_ids)} вакансий как опубликованные")
        
        # Логируем детали публикации
        companies = {}
        for vacancy in vacancies:
            employer = vacancy.get('employer', 'Не указано')
            companies[employer] = companies.get(employer, 0) + 1
        
        logger.info(f"Детали публикации для {city_info['name']}:")
        logger.info(f"  - Всего вакансий: {len(vacancies)}")
        logger.info(f"  - Уникальных компаний: {len(companies)}")
        for employer, count in sorted(companies.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  - {employer}: {count} вакансий")
        
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
        logger.error("   Проверьте настройки GitHub Secrets:")
        logger.error("   - SUPABASE_URL")
        logger.error("   - SUPABASE_KEY") 
        logger.error("   - TG_BOT_TOKEN")
        return False, {}
    
    # Проверяем, нужно ли публиковать (только для локального запуска)
    if not should_publish_now():
        logger.info("⏸️  Не время для публикации по расписанию")
        logger.info("   В режиме GitHub Actions публикация всегда разрешена")
        return True, {}
    
    # Проверяем доступность supabase
    if not SUPABASE_AVAILABLE:
        logger.error("❌ ОШИБКА: Библиотека supabase не установлена")
        logger.error("   Установите: pip install supabase==1.1.1")
        return False, {}
    
    # Подключаемся к Supabase
    try:
        logger.info("\n🔗 Подключаюсь к Supabase...")
        supabase_client = create_client(supabase_url, supabase_key)
        
        # Делаем тестовый запрос для проверки подключения
        test_result = supabase_client.table("vacancies").select("id", count="exact").limit(1).execute()
        logger.info(f"✅ Успешное подключение к Supabase")
        logger.info(f"   Всего записей в базе: {test_result.count if hasattr(test_result, 'count') else 'неизвестно'}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Supabase: {str(e)}")
        logger.error("   Проверьте:")
        logger.error("   1. Правильность SUPABASE_URL")
        logger.error("   2. Правильность SUPABASE_KEY")
        logger.error("   3. Доступность базы данных")
        logger.error("   4. Права доступа ключа")
        return False, {}
    
    # Публикуем для каждого города
    logger.info(f"\n📍 ПУБЛИКАЦИЯ ДЛЯ {len(CITIES)} ГОРОДОВ")
    logger.info(f"   Целевое количество вакансий: {PUBLISH_CONFIG['publication']['vacancies_per_post']}")
    logger.info(f"   Максимум от одной компании: {PUBLISH_CONFIG.get('filters', {}).get('max_vacancies_per_company', 2)}")
    logger.info(f"   Время публикации: {datetime.now(timezone(timedelta(hours=3))).strftime('%H:%M %d.%m.%Y')}")
    
    results = {}
    all_success = True
    total_vacancies = 0
    cities_processed = 0
    cities_with_vacancies = 0
    
    for city_slug in CITIES.keys():
        city_name = CITIES[city_slug]["name"]
        logger.info(f"\n{'='*50}")
        logger.info(f"📍 ГОРОД: {city_name.upper()} ({city_slug})")
        logger.info(f"   Канал: {CITIES[city_slug]['channel']}")
        
        try:
            success, message, count = publish_city_vacancies(
                supabase_client,
                bot_token,
                city_slug
            )
            
            results[city_slug] = count
            total_vacancies += count
            cities_processed += 1
            
            if success:
                if count > 0:
                    logger.info(f"✅ УСПЕХ: {message}")
                    cities_with_vacancies += 1
                else:
                    logger.info(f"ℹ️  ИНФО: {message}")
            else:
                logger.error(f"❌ ОШИБКА: {message}")
                all_success = False
            
            # Небольшая задержка между городами
            import time
            if cities_processed < len(CITIES):  # Не ждём после последнего города
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ НЕОЖИДАННАЯ ОШИБКА в {city_name}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            results[city_slug] = 0
            all_success = False
    
    # Итоговая статистика
    logger.info(f"\n{'='*60}")
    logger.info("📊 ИТОГИ ПУБЛИКАЦИИ:")
    logger.info(f"{'='*60}")
    
    # Статистика по городам
    for city_slug, count in results.items():
        city_name = CITIES[city_slug]["name"]
        channel = CITIES[city_slug]["channel"]
        status = "✅" if count > 0 else "ℹ️ " if count == 0 else "❌"
        logger.info(f"{status} {city_name:20} | {count:2} вакансий | {channel}")
    
    # Общая статистика
    logger.info(f"{'─'*60}")
    logger.info(f"📈 ВСЕГО ОПУБЛИКОВАНО: {total_vacancies} вакансий")
    logger.info(f"🏙️  ГОРОДОВ ОБРАБОТАНО: {cities_processed}/{len(CITIES)}")
    logger.info(f"📍 ГОРОДОВ С ВАКАНСИЯМИ: {cities_with_vacancies}/{len(CITIES)}")
    
    if total_vacancies == 0:
        logger.info("\nℹ️  Новых вакансий для публикации не найдено")
        logger.info("   Возможные причины:")
        logger.info("   1. Все вакансии уже опубликованы (is_posted = TRUE)")
        logger.info("   2. Нет новых вакансий за последние 7 дней")
        logger.info("   3. Вакансии не в рублях (currency != 'RUR')")
        logger.info("   4. Ошибка в критериях отбора")
        logger.info("   5. Проблемы с подключением к базе данных")
    else:
        avg_vacancies = total_vacancies / cities_with_vacancies if cities_with_vacancies > 0 else 0
        logger.info(f"📊 СРЕДНЕЕ НА ГОРОД: {avg_vacancies:.1f} вакансий")
    
    logger.info(f"{'='*60}")
    
    # Проверяем общий успех
    if not all_success:
        logger.error("❌ Публикация завершена с ошибками в одном или нескольких городах")
        logger.error("   Проверьте логи выше для деталей")
    elif total_vacancies == 0:
        logger.info("✅ Публикация завершена успешно, но вакансий не найдено")
    else:
        logger.info("✅ Публикация завершена успешно!")
    
    # Возвращаем результат
    # Даже если нет вакансий, но процесс выполнился корректно - это успех
    process_success = all_success  # True если не было критических ошибок
    
    return process_success, results


if __name__ == "__main__":
    """
    Точка входа для запуска из командной строки.
    """
    import sys
    
    try:
        # Запускаем публикацию
        success, stats = main_publisher()
        
        # Возвращаем код выхода для GitHub Actions
        if success:
            logger.info("✅ Публикация завершена успешно")
            sys.exit(0)
        else:
            logger.error("❌ Публикация завершена с ошибками")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("\n⚠️  Публикация прервана пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
