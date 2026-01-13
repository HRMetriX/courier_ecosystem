# Конфигурация публикатора
from datetime import datetime, timedelta, timezone

# Основные настройки
PUBLISH_CONFIG = {
    # Критерии отбора вакансий
    "criteria": {
        "max_vacancy_age_days": 30,
        "max_parsed_age_days": 14,
        "currency": "RUR",
      #  "min_salary_net": 70000,
    },
    
    # Параметры публикации
    "publication": {
        "vacancies_per_post": 10,  # Целевое количество вакансий
        "post_times_msk": ["09:00", "13:00", "19:00", "21:00"],
    },
    
    # Фильтры
    "filters": {
        "max_vacancies_per_company": 2,  # Максимум вакансий от одной компании
    },
    
    # Форматирование
    "formatting": {
        "emojis": {
            "title": "🚴",
            "salary": "💰",
            "company": "🏢",
            "date": "📅",
            "payment": "💳",
            "employer": "✅",
            "divider": "---",
            "schedule": "🕒",
            "experience": "📊",
        },
        "referral_link": "https://ya.cc/8UiUqj",
    },
    
    # Telegram
    "telegram": {
        "bot_token_env": "TG_BOT_TOKEN",
        "publisher_bot": "@courier_publisher_bot",
    },
}

# Города и каналы
CITIES = {
    "msk": {"channel": "@courier_jobs_msk", "name": "Москва"},
    "spb": {"channel": "@courier_jobs_spb", "name": "Санкт-Петербург"},
    "nsk": {"channel": "@courier_jobs_nsk", "name": "Новосибирск"},
    "ekb": {"channel": "@courier_jobs_ekb", "name": "Екатеринбург"},
    "kzn": {"channel": "@courier_jobs_kzn", "name": "Казань"},
    "nng": {"channel": "@courier_jobs_nng", "name": "Нижний Новгород"},
    "che": {"channel": "@courier_jobs_che", "name": "Челябинск"},
    "krk": {"channel": "@courier_jobs_krk", "name": "Красноярск"},
}

def get_current_time_msk():
    """Возвращает текущее время по Москве."""
    return datetime.now().astimezone(timezone(timedelta(hours=3)))
