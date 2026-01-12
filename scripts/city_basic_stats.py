import os
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from io import BytesIO
from supabase import create_client
import asyncio
from telegram import Bot
from typing import Dict, List

# Конфигурация
CITIES = {
    "msk": {"channel": "@courier_jobs_msk", "name": "Москва"},
    "spb": {"channel": "@courier_jobs_spb", "name": "Санкт-Петербург"},
    "nsk": {"channel": "@courier_jobs_nsk", "name": "Новосибирск"},
    "ekb": {"channel": "@courier_jobs_ekb", "name": "Екатеринбург"},
    "kzn": {"channel": "@courier_jobs_kzn", "name": "Казань"},
}

def load_data_from_supabase():
    """Загрузка всех данных из Supabase"""
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("Не найдены переменные окружения SUPABASE_URL или SUPABASE_KEY")
    
    supabase_client = create_client(supabase_url, supabase_key)
    
    # Загрузка ВСЕХ данных с пагинацией
    all_data = []
    page = 0
    limit = 1000  # Максимальный лимит за запрос

    while True:
        response = supabase_client.table("vacancies").select("*").range(
            page * limit, (page + 1) * limit - 1
        ).execute()

        if not response.data:
            break

        all_data.extend(response.data)
        page += 1
        print(f"Загружено страниц: {page}, всего строк: {len(all_data)}")

    df = pd.DataFrame(all_data)
    print(f"\n✅ Итого загружено {len(df)} строк")
    
    # Преобразуем колонки для совместимости
    if 'published_at' in df.columns:
        # Преобразуем published_at в московское время и берем ТОЛЬКО ДАТУ
        df['published_at'] = pd.to_datetime(df['published_at'])
        moscow_tz = 'Europe/Moscow'
        df['published_at_moscow'] = df['published_at'].dt.tz_convert(moscow_tz)
        df['published_date'] = df['published_at_moscow'].dt.date
    elif 'published_date' not in df.columns:
        # Если ни одной колонки нет, создаем пустую
        df['published_date'] = pd.NaT
    
    return df

def create_digest_image(city_name: str, city_data: pd.DataFrame, today_date: datetime):
    """Создание изображения дайджеста для конкретного города"""
    
    # Устанавливаем шрифты
    plt.rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['axes.unicode_minus'] = False
    
    # Используем исправленный фильтр для зарплат
    def contains_monthly_pattern(text):
        if pd.isna(text):
            return False
        text_lower = str(text).lower()
        patterns = ['месяц', 'month', 'мес', 'ежемесячно', 'в месяц', 'per month', 'месячный']
        return any(pattern in text_lower for pattern in patterns)
    
    city_salary_data = city_data[
        city_data['salary_period_name'].apply(contains_monthly_pattern) & 
        city_data['salary_to_net'].notna()
    ]
    
    # Вычисляем даты
    yesterday_date = today_date - timedelta(days=1)
    week_start_date = today_date - timedelta(days=6)
    
    # СТАТИСТИКА ЗА СЕГОДНЯ
    city_today = city_data[city_data['published_date'] == today_date.date()]
    today_count = len(city_today)
    
    city_yesterday = city_data[city_data['published_date'] == yesterday_date.date()]
    yesterday_count = len(city_yesterday)
    
    # СТАТИСТИКА ЗА НЕДЕЛЮ
    city_week = city_data[city_data['published_date'] >= week_start_date.date()]
    city_salary_week = city_salary_data[city_data['published_date'] >= week_start_date.date()]  # ИСПРАВЛЕНО
    
    # ЗАРПЛАТНАЯ СТАТИСТИКА ЗА НЕДЕЛЮ
    weekly_salary_stats = []
    if len(city_salary_week) > 0:
        # Группируем по дням
        for day in pd.date_range(week_start_date.date(), today_date.date()):
            day_date = day.date()
            day_data = city_salary_week[city_salary_week['published_date'] == day_date]
            if len(day_data) > 0:
                weekly_salary_stats.append({
                    'date': day_date,
                    'avg_salary': day_data['salary_to_net'].mean(),
                    'median_salary': day_data['salary_to_net'].median(),
                    'vacancy_count': len(day_data)
                })
    
    # СОЗДАЕМ ГРАФИК - только 2 графика
    fig = plt.figure(figsize=(12, 8), facecolor='white')
    
    # СЕТКА для двух графиков
    gs = fig.add_gridspec(2, 1, hspace=0.4, wspace=0.3)
    
    # 1. ЗАРПЛАТНАЯ ДИНАМИКА ЗА НЕДЕЛЮ (верхний график)
    if len(weekly_salary_stats) >= 2:
        ax_salary_trend = fig.add_subplot(gs[0, 0])
        
        dates = [s['date'].strftime('%d.%m') for s in weekly_salary_stats]
        avg_salaries = [s['avg_salary'] for s in weekly_salary_stats]
        median_salaries = [s['median_salary'] for s in weekly_salary_stats]
        
        # Вычисляем среднее значение за весь период для горизонтальной линии
        overall_avg_salary = sum(avg_salaries) / len(avg_salaries)
        
        # Линия средних зарплат
        ax_salary_trend.plot(dates, avg_salaries, 'o-', linewidth=3, 
                           markersize=8, color='#3498db', label='Средняя', alpha=0.8)
        
        # Линия медианных зарплат
        ax_salary_trend.plot(dates, median_salaries, 's--', linewidth=2,
                           markersize=6, color='#2ecc71', label='Медиана', alpha=0.8)
        
        # Горизонтальная линия среднего за весь период
        ax_salary_trend.axhline(y=overall_avg_salary, color='red', linestyle=':', linewidth=2, 
                               label=f'Среднее за период: {overall_avg_salary:,.0f} ₽', alpha=0.7)
        
        ax_salary_trend.set_title(f'ДИНАМИКА ЗАРПЛАТ ЗА НЕДЕЛЮ - {city_name.upper()}', 
                                fontsize=12, fontweight='bold', pad=10)
        ax_salary_trend.set_ylabel('Рубли', fontsize=10)
        ax_salary_trend.tick_params(axis='x', rotation=45)
        ax_salary_trend.grid(True, alpha=0.3, color='lightgray', linestyle='-', linewidth=0.5)
        ax_salary_trend.legend(loc='upper left')
        
        # Убираем заливку фона
        ax_salary_trend.set_facecolor('white')
        
        # Форматируем оси Y
        ax_salary_trend.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    else:
        ax_salary_trend = fig.add_subplot(gs[0, 0])
        ax_salary_trend.axis('off')
        ax_salary_trend.text(0.5, 0.5, f'Недостаточно данных\nдля графика зарплат в {city_name}', 
                           ha='center', va='center', fontsize=12, color='#7f8c8d')
    
    # 2. АКТИВНОСТЬ ЗА НЕДЕЛЮ (нижний график)
    if len(city_week) > 0:
        ax_activity = fig.add_subplot(gs[1, 0])
        
        daily_activity = city_week.groupby('published_date').size()
        dates_activity = [d.strftime('%d.%m') for d in daily_activity.index]
        
        bars = ax_activity.bar(dates_activity, daily_activity.values, 
                              color='#9b59b6', alpha=0.7, edgecolor='white')
        
        # Подсвечиваем сегодня
        today_str = today_date.strftime('%d.%m')
        if today_str in dates_activity:
            today_idx = dates_activity.index(today_str)
            bars[today_idx].set_color('#e74c3c')
            bars[today_idx].set_alpha(1.0)
        
        ax_activity.set_title('ВАКАНСИИ ЗА НЕДЕЛЮ', 
                            fontsize=12, fontweight='bold', pad=10)
        ax_activity.set_ylabel('Количество', fontsize=10)
        ax_activity.tick_params(axis='x', rotation=45)
        ax_activity.grid(True, alpha=0.3, axis='y', color='lightgray', linestyle='-', linewidth=0.5)
        
        # Убираем заливку фона
        ax_activity.set_facecolor('white')
        
    else:
        ax_activity = fig.add_subplot(gs[1, 0])
        ax_activity.axis('off')
        ax_activity.text(0.5, 0.5, 'Нет данных\nза неделю', 
                        ha='center', va='center', fontsize=12, color='#7f8c8d')
    
    # Убираем заливку всего холста
    fig.patch.set_facecolor('white')
    
    plt.tight_layout()
    
    # Сохраняем в буфер
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    
    buf.seek(0)
    return buf

def generate_telegram_text(city_name: str, city_data: pd.DataFrame, today_date: datetime):
    """Генерация текста дайджеста для Telegram"""
    
    # Используем исправленный фильтр для зарплат
    def contains_monthly_pattern(text):
        if pd.isna(text):
            return False
        text_lower = str(text).lower()
        patterns = ['месяц', 'month', 'мес', 'ежемесячно', 'в месяц', 'per month', 'месячный']
        return any(pattern in text_lower for pattern in patterns)
    
    city_salary_data = city_data[
        city_data['salary_period_name'].apply(contains_monthly_pattern) & 
        city_data['salary_to_net'].notna()
    ]
    
    # Вычисляем даты
    yesterday_date = today_date - timedelta(days=1)
    week_start_date = today_date - timedelta(days=6)
    
    # СТАТИСТИКА ЗА СЕГОДНЯ
    city_today = city_data[city_data['published_date'] == today_date.date()]
    today_count = len(city_today)
    
    city_yesterday = city_data[city_data['published_date'] == yesterday_date.date()]
    yesterday_count = len(city_yesterday)
    
    # СТАТИСТИКА ЗА НЕДЕЛЮ
    city_week = city_data[city_data['published_date'] >= week_start_date.date()]
    city_salary_week = city_salary_data[city_data['published_date'] >= week_start_date.date()]  # ИСПРАВЛЕНО
    
    # РАБОТОДАТЕЛИ
    top_employers_today = city_today['employer'].value_counts().head(3)
    
    # КАЧЕСТВО ДАННЫХ
    salary_coverage_week = (city_week['salary_to_net'].notna().sum() / len(city_week) * 100) if len(city_week) > 0 else 0
    
    # ЗАРПЛАТНАЯ СТАТИСТИКА НА СЕГОДНЯ
    salary_today = city_salary_data[city_salary_data['published_date'] == today_date.date()]
    
    # Формируем текст для Telegram
    daily_growth = today_count - yesterday_count
    daily_growth_pct = (daily_growth / yesterday_count * 100) if yesterday_count > 0 else (float('inf') if today_count > 0 else 0)
    
    # Определяем вердикт
    if today_count == 0:
        verdict = "🔴 НЕТ НОВЫХ ВАКАНСИЙ"
        verdict_color = "🔴"
    elif daily_growth_pct > 50:
        verdict = "🟢 БУРНЫЙ РОСТ"
        verdict_color = "🟢"
    elif daily_growth_pct > 10:
        verdict = "🟢 ХОРОШИЙ РОСТ"
        verdict_color = "🟢"
    elif daily_growth_pct < -30:
        verdict = "🔴 СИЛЬНЫЙ СПАД"
        verdict_color = "🔴"
    elif daily_growth_pct < 0:
        verdict = "🟡 НЕБОЛЬШОЙ СПАД"
        verdict_color = "🟡"
    else:
        verdict = "🟡 СТАБИЛЬНО"
        verdict_color = "🟡"
    
    telegram_text = f"""📊 ДАЙДЖЕСТ РЫНКА ВАКАНСИЙ | {city_name.upper()}
📅 {today_date.strftime('%d.%m.%Y')}

📈 ОСНОВНЫЕ ПОКАЗАТЕЛИ:
• Сегодня: {today_count:,} вакансий ({daily_growth:+,d}, {daily_growth_pct:+.1f}%)
• За неделю: {len(city_week):,} вакансий
• С зарплатой: {len(city_salary_week):,} вакансий
• Покрытие зарплатами: {salary_coverage_week:.0f}%

💰 ЗАРПЛАТЫ СЕГОДНЯ ({len(salary_today):,} вакансий):
• Средняя: {salary_today['salary_to_net'].mean():,.0f} ₽
• Медианная: {salary_today['salary_to_net'].median():,.0f} ₽
• 25% получают до: {salary_today['salary_to_net'].quantile(0.25):,.0f} ₽
• 75% получают до: {salary_today['salary_to_net'].quantile(0.75):,.0f} ₽

🏢 ТОП РАБОТОДАТЕЛИ СЕГОДНЯ:
"""
    
    for i, (employer, count) in enumerate(top_employers_today.items(), 1):
        employer_short = employer[:25] + '...' if len(employer) > 25 else employer
        telegram_text += f"{i}. {employer_short} - {count:,} вакансий\n"
    
    telegram_text += f"""

🎯 ВЕРДИКТ ДНЯ: {verdict_color} {verdict}

📊 ВСЕГО В {city_name.upper()}:
• Вакансий: {len(city_data):,}
• С зарплатой: {len(city_salary_data):,}
• Средняя зарплата: {city_salary_data['salary_to_net'].mean():,.0f} ₽
• Период: {city_data['published_date'].min()} - {today_date.date()}

⏰ Обновлено: {datetime.now().strftime('%H:%M')}
"""
    
    return telegram_text

async def send_digest_to_channel(bot_token: str, channel: str, image_buf: BytesIO, text: str):
    """Отправка дайджеста в Telegram канал"""
    bot = Bot(token=bot_token)
    
    # Отправляем изображение с подписью
    image_buf.seek(0)
    await bot.send_photo(chat_id=channel, photo=image_buf, caption=text)
    
    print(f"✅ Дайджест отправлен в канал {channel}")

async def main():
    """Основная асинхронная функция"""
    print("🚀 Запуск генерации ежедневных дайджестов...")
    
    # Загружаем данные
    print("📦 Загружаем данные из Supabase...")
    df = load_data_from_supabase()
    
    # Проверяем наличие нужных столбцов
    required_columns = ['city_slug', 'published_date', 'salary_period_name', 'salary_to_net', 'employer']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Доступные колонки: {list(df.columns)}")
        raise ValueError(f"Отсутствуют столбцы: {missing_columns}")
    
    # Приводим published_date к datetime
    df['published_date'] = pd.to_datetime(df['published_date']).dt.date
    
    # Получаем токен бота
    bot_token = os.environ.get("TG_BOT_TOKEN")
    if not bot_token:
        raise ValueError("Не найдена переменная окружения TG_BOT_TOKEN")
    
    # Текущая дата
    today_date = datetime.now()
    
    # Проходим по каждому городу
    for city_slug, city_info in CITIES.items():
        print(f"\n📍 Обработка города: {city_info['name']} ({city_slug})")
        
        # Фильтруем данные по городу
        city_data = df[df['city_slug'] == city_slug]
        
        if len(city_data) == 0:
            print(f"⚠️ Нет данных для города {city_info['name']}")
            continue
        
        try:
            # Создаем изображение
            print(f"🎨 Генерируем изображение для {city_info['name']}...")
            image_buf = create_digest_image(city_info['name'], city_data, today_date)
            
            # Генерируем текст
            print(f"📝 Генерируем текст для {city_info['name']}...")
            text = generate_telegram_text(city_info['name'], city_data, today_date)
            
            # Отправляем в канал
            print(f"📤 Отправляем дайджест в канал {city_info['channel']}...")
            await send_digest_to_channel(bot_token, city_info['channel'], image_buf, text)
            
            print(f"✅ Дайджест для {city_info['name']} успешно отправлен!")
            
        except Exception as e:
            print(f"❌ Ошибка при обработке {city_info['name']}: {str(e)}")
            continue
    
    print(f"\n🎉 Все дайджесты успешно отправлены!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
