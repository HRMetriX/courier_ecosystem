import os
import sys
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from supabase import create_client

# ================= КОНФИГУРАЦИЯ =================
HH_API_URL = "https://api.hh.ru/vacancies"
BASE_PARAMS = {
    "text": "Курьер",
    "search_field": "name",
    "professional_role": 58,
    "per_page": 100,
    "only_with_salary": False,
}

CITIES = {
    "msk": {"area_id": 1, "channel": "@courier_jobs_msk"},
    "spb": {"area_id": 2, "channel": "@courier_jobs_spb"},
    "nsk": {"area_id": 4, "channel": "@courier_jobs_nsk"},
    "ekb": {"area_id": 3, "channel": "@courier_jobs_ekb"},
    "kzn": {"area_id": 88, "channel": "@courier_jobs_kzn"},
    "nng": {"area_id": 66, "channel": "@courier_jobs_nng"},
    "che": {"area_id": 104, "channel": "@courier_jobs_che"},
    "krk": {"area_id": 54, "channel": "@courier_jobs_krk"},
}

# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================
def get_all_industries() -> List[Dict]:
    """Получает список всех индустрий верхнего уровня с HH."""
    print("📋 Получаю список индустрий с HH...")
    response = requests.get("https://api.hh.ru/industries")
    response.raise_for_status()
    data = response.json()
    print(f"   Найдено {len(data)} индустрий.")
    return data

def calculate_net_salary(salary_value: Optional[int], gross: Optional[bool]) -> Optional[int]:
    """Рассчитывает зарплату 'на руки' в рублях."""
    if salary_value is None:
        return None
    if gross is True:
        return int(salary_value * 0.87)
    return salary_value

def format_vacancy(vacancy: Dict, city_slug: str) -> Dict:
    """Форматирует вакансию для базы."""
    try:
        salary = vacancy.get("salary")
        
        salary_from = None
        salary_to = None
        currency = None
        gross = None
        
        if salary and isinstance(salary, dict):
            salary_from = salary.get("from")
            salary_to = salary.get("to")
            currency = salary.get("currency")
            gross = salary.get("gross")
        
        # Рассчитываем net-зарплаты (только для RUR)
        salary_from_net = calculate_net_salary(salary_from, gross) if currency == "RUR" else None
        salary_to_net = calculate_net_salary(salary_to, gross) if currency == "RUR" else None
        
        # Период и частота выплат
        salary_range = vacancy.get('salary_range', {})
        mode = salary_range.get('mode', {}) if isinstance(salary_range, dict) else {}
        frequency = salary_range.get('frequency', {}) if isinstance(salary_range, dict) else {}
        
        # График работы
        schedule = vacancy.get('schedule', {})
        
        work_schedule_by_days = None
        if vacancy.get('work_schedule_by_days') and isinstance(vacancy['work_schedule_by_days'], list):
            if vacancy['work_schedule_by_days']:
                work_schedule_by_days = vacancy['work_schedule_by_days'][0].get('name')
        
        working_hours = None
        if vacancy.get('working_hours') and isinstance(vacancy['working_hours'], list):
            if vacancy['working_hours']:
                working_hours = vacancy['working_hours'][0].get('name')
        
        # Работодатель
        employer = vacancy.get('employer', {})
        
        return {
            # Базовые поля
            "external_id": str(vacancy.get("id", "")),
            "source": "hh",
            "title": vacancy.get("name", ""),
            "employer": employer.get("name", "") if isinstance(employer, dict) else "",
            "employer_trusted": employer.get("trusted") if isinstance(employer, dict) else None,
            
            # Зарплата оригинальная
            "salary_from": salary_from,
            "salary_to": salary_to,
            "currency": currency,
            "gross": gross,
            
            # Зарплата net
            "salary_from_net": salary_from_net,
            "salary_to_net": salary_to_net,
            
            # Период и частота выплат
            "salary_period_id": mode.get('id') if isinstance(mode, dict) else None,
            "salary_period_name": mode.get('name') if isinstance(mode, dict) else None,
            "salary_frequency_id": frequency.get('id') if isinstance(frequency, dict) else None,
            "salary_frequency_name": frequency.get('name') if isinstance(frequency, dict) else None,
            
            # График работы
            "schedule_name": schedule.get('name') if isinstance(schedule, dict) else None,
            "work_schedule_by_days": work_schedule_by_days,
            "working_hours": working_hours,
            
            # Опыт и занятость
            "experience_name": vacancy.get('experience', {}).get('name') if isinstance(vacancy.get('experience'), dict) else None,
            "employment_form_name": vacancy.get('employment_form', {}).get('name') if isinstance(vacancy.get('employment_form'), dict) else None,
            
            # Локация
            "city": vacancy.get("area", {}).get("name", "") if isinstance(vacancy.get('area'), dict) else "",
            "city_slug": city_slug,
            "channel_id": CITIES.get(city_slug, {}).get("channel", ""),
            
            # Временные метки
            # Для first_seen_in_db используем published_at из HH
            "published_at": vacancy.get("published_at", ""),
            "first_seen_in_db": vacancy.get("published_at", ""), # <-- Добавляем published_at как first_seen_in_db
            
            # Ссылки
            "external_url": vacancy.get("alternate_url", ""),
        }
    
    except Exception as e:
        # Минимальные данные при ошибке
        # first_seen_in_db устанавливаем из published_at
        return {
            "external_id": str(vacancy.get("id", "")),
            "source": "hh",
            "title": str(vacancy.get("name", "")),
            "employer": str(vacancy.get("employer", {}).get("name", "")),
            "city_slug": city_slug,
            "published_at": vacancy.get("published_at", ""),
            "first_seen_in_db": vacancy.get("published_at", ""), # <-- Также добавляем сюда
            "external_url": vacancy.get("alternate_url", ""),
        }

def fetch_vacancies(params: Dict) -> List[Dict]:
    """Запрашивает все страницы результатов."""
    all_vacancies = []
    page = 0
    
    while True:
        params_copy = params.copy()
        params_copy["page"] = page
        
        try:
            response = requests.get(HH_API_URL, params=params_copy, timeout=30)
            
            if response.status_code == 400 and "2000" in response.text:
                raise requests.exceptions.HTTPError("Превышен лимит 2000 вакансий", response=response)
            
            response.raise_for_status()
            data = response.json()
            page_vacancies = data.get("items", [])
            all_vacancies.extend(page_vacancies)
            
            total_pages = data.get("pages", 1)
            if page >= total_pages - 1 or page >= 19:
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"     ❌ Ошибка на странице {page}: {str(e)}")
            break
    
    return all_vacancies

def process_industry(area_id: int, industry_id: str, date_from, date_to) -> List[Dict]:
    """Обрабатывает поиск по индустрии с разбивкой по дням."""
    industry_params = {**BASE_PARAMS, "area": area_id, "industry": industry_id}
    all_vacancies = []
    
    params = {
        **industry_params,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat()
    }
    
    try:
        all_vacancies = fetch_vacancies(params)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400 and "2000" in str(e):
            for single_date in [date_from, date_to]:
                date_params = {
                    **industry_params,
                    "date_from": single_date.isoformat(),
                    "date_to": single_date.isoformat()
                }
                try:
                    day_vacancies = fetch_vacancies(date_params)
                    all_vacancies.extend(day_vacancies)
                except requests.exceptions.HTTPError as e2:
                    if e2.response.status_code == 400 and "2000" in str(e2):
                        continue
                    else:
                        raise
        else:
            raise
    
    return all_vacancies

def upsert_vacancy(supabase_client, vacancy_data: Dict):
    """Вставляет или обновляет вакансию в базе."""
    # Убедимся, что last_updated_in_db не передаётся из Python, пусть управляется триггером
    # Исключим last_updated_in_db из данных для отправки
    data_to_send = {k: v for k, v in vacancy_data.items() if k != "last_updated_in_db"}

    # Удаляем first_seen_in_db из data_to_send, так как его устанавливает триггер при INSERT
    # и не должен менять при UPDATE
    data_to_send.pop("first_seen_in_db", None) # Используем pop с default None, на всякий случай

    try:
        supabase_client.table("vacancies").insert(data_to_send).execute()
        return "inserted"
    except Exception as e:
        if "duplicate key" in str(e).lower() or "23505" in str(e):
            # При UPDATE НЕ передаём first_seen_in_db
            # Убедимся, что first_seen_in_db и last_updated_in_db не входят в update_data
            update_data = {k: v for k, v in data_to_send.items() if k not in ["first_seen_in_db", "last_updated_in_db"]}
            supabase_client.table("vacancies").update(update_data).eq(
                "external_id", data_to_send["external_id"]
            ).eq("source", "hh").execute()
            return "updated"
        else:
            print(f"     ❌ Ошибка UPSERT: {str(e)[:100]}")
            return "error"

# ================= ОСНОВНАЯ ЛОГИКА =================
def main():
    """Основная логика парсера."""
    print("=" * 60)
    print("🚀 ПАРСЕР HH.RU")
    print("=" * 60)
    
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("❌ Ошибка: SUPABASE_URL и SUPABASE_KEY должны быть установлены")
        sys.exit(1)
    
    supabase_client = create_client(supabase_url, supabase_key)
    print("✅ Подключение к Supabase установлено")
    
    industries = get_all_industries()
    
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    print(f"📅 Период поиска: {yesterday} - {today}")
    
    stats = {"inserted": 0, "updated": 0, "error": 0}
    
    for city_slug, city_config in CITIES.items():
        print(f"\n📍 ГОРОД: {city_slug.upper()} (area_id={city_config['area_id']})")
        city_vacancies_raw = []
        
        base_params = {
            **BASE_PARAMS,
            "area": city_config["area_id"],
            "date_from": yesterday.isoformat(),
            "date_to": today.isoformat()
        }
        
        try:
            city_vacancies_raw = fetch_vacancies(base_params)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "2000" in str(e):
                for industry in industries:
                    industry_vacancies = process_industry(
                        city_config["area_id"], 
                        industry["id"], 
                        yesterday, 
                        today
                    )
                    city_vacancies_raw.extend(industry_vacancies)
            else:
                continue
        
        city_processed = 0
        
        for vac in city_vacancies_raw:
            try:
                formatted_vac = format_vacancy(vac, city_slug)
                result = upsert_vacancy(supabase_client, formatted_vac)
                stats[result] += 1
                city_processed += 1
                
                if city_processed % 100 == 0:
                    print(f"     ⏳ Обработано {city_processed} вакансий...")
                    
            except Exception as e:
                stats["error"] += 1
                continue
        
        print(f"   ✅ {city_slug.upper()}: {len(city_vacancies_raw)} сырых -> {city_processed} обработано")
    
    print("\n" + "=" * 60)
    print("📊 ИТОГИ:")
    print("=" * 60)
    
    total_processed = stats["inserted"] + stats["updated"] + stats["error"]
    print(f"Всего обработано: {total_processed}")
    print(f"  ✅ Вставлено новых: {stats['inserted']}")
    print(f"  🔄 Обновлено существующих: {stats['updated']}")
    print(f"  ❌ Ошибок: {stats['error']}")
    
    try:
        count_result = supabase_client.table("vacancies").select("id", count="exact").execute()
        print(f"\n📈 Всего записей в базе: {count_result.count}")
    except:
        pass
    
    print("\n✅ Загрузка завершена!")

if __name__ == "__main__":
    main()
