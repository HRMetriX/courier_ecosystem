import os
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from io import BytesIO
from supabase import create_client
import asyncio
from telegram import Bot
import pytz  # Добавляем импорт

# Константа для московского часового пояса
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Конфигурация
CITIES = {
    "msk": {"channel": "@courier_jobs_msk", "name": "Москва"},
    "spb": {"channel": "@courier_jobs_spb", "name": "Санкт-Петербург"},
    "nsk": {"channel": "@courier_jobs_nsk", "name": "Новосибирск"},
    "ekb": {"channel": "@courier_jobs_ekb", "name": "Екатеринбург"},
    "kzn": {"channel": "@courier_jobs_kzn", "name": "Казань"},
    # ↓ ДОБАВИТЬ 3 НОВЫХ ГОРОДА ↓
    "nng": {"channel": "@courier_jobs_nng", "name": "Нижний Новгород"},
    "che": {"channel": "@courier_jobs_che", "name": "Челябинск"},
    "krk": {"channel": "@courier_jobs_krk", "name": "Красноярск"},
}

# Вспомогательные функции
def contains_monthly_pattern(text):
    """Проверяет, содержит ли текст указание на месячные выплаты"""
    if pd.isna(text):
        return False
    text_lower = str(text).lower()
    patterns = ['месяц', 'month', 'мес', 'ежемесячно', 'в месяц', 'per month', 'месячный']
    return any(pattern in text_lower for pattern in patterns)

def safe_format_number(value, suffix=" ₽"):
    """Безопасное форматирование чисел с обработкой NaN"""
    if pd.isna(value) or value is None:
        return "нет данных"
    try:
        return f"{value:,.0f}{suffix}"
    except:
        return "ошибка"

def get_comparison_dates(today_date):
    """Возвращает даты для корректного сравнения"""
    return {
        'today': today_date.date(),
        'yesterday': (today_date - timedelta(days=1)).date(),
        'day_before': (today_date - timedelta(days=2)).date(),
        'week_start': (today_date - timedelta(days=6)).date()
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
    limit = 1000

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
        # Безопасное преобразование дат
        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
        # Конвертируем в московское время
        df['published_at_moscow'] = df['published_at'].dt.tz_convert(MOSCOW_TZ)
        df['published_date'] = df['published_at_moscow'].dt.date
    elif 'published_date' not in df.columns:
        df['published_date'] = pd.NaT
    
    return df

def create_digest_image(city_name: str, city_data: pd.DataFrame, today_date: datetime):
    """Создание изображения дайджеста для конкретного города"""
    
    # Устанавливаем шрифты
    plt.rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['axes.unicode_minus'] = False
    
    # Фильтруем зарплатные данные
    city_salary_data = city_data[
        city_data['salary_period_name'].apply(contains_monthly_pattern) & 
        city_data['salary_to_net'].notna()
    ]
    
    # Получаем даты для сравнения
    dates = get_comparison_dates(today_date)
    
    # Данные за неделю (для графиков)
    city_week = city_data[city_data['published_date'] >= dates['week_start']]
    city_salary_week = city_salary_data[city_salary_data['published_date'] >= dates['week_start']]
    
    # ЗАРПЛАТНАЯ СТАТИСТИКА ЗА НЕДЕЛЮ
    weekly_salary_stats = []
    if len(city_salary_week) > 0:
        for day in pd.date_range(dates['week_start'], dates['today']):
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
    gs = fig.add_gridspec(2, 1, hspace=0.4, wspace=0.3)
    
    # 1. ЗАРПЛАТНАЯ ДИНАМИКА ЗА НЕДЕЛЮ (верхний график)
    if len(weekly_salary_stats) >= 2:
        ax_salary_trend = fig.add_subplot(gs[0, 0])
        
        dates_str = [s['date'].strftime('%d.%m') for s in weekly_salary_stats]
        avg_salaries = [s['avg_salary'] for s in weekly_salary_stats]
        median_salaries = [s['median_salary'] for s in weekly_salary_stats]
        
        # Вычисляем среднее значение за весь период
        overall_avg_salary = sum(avg_salaries) / len(avg_salaries) if avg_salaries else 0
        
        # Линия средних зарплат
        ax_salary_trend.plot(dates_str, avg_salaries, 'o-', linewidth=3, 
                           markersize=8, color='#3498db', label='Средняя', alpha=0.8)
        
        # Линия медианных зарплат
        ax_salary_trend.plot(dates_str, median_salaries, 's--', linewidth=2,
                           markersize=6, color='#2ecc71', label='Медиана', alpha=0.8)
        
        # Горизонтальная линия среднего за весь период
        if overall_avg_salary > 0:
            ax_salary_trend.axhline(y=overall_avg_salary, color='red', linestyle=':', linewidth=2, 
                                   label=f'Среднее за период: {overall_avg_salary:,.0f} ₽', alpha=0.7)
        
        ax_salary_trend.set_title(f'ДИНАМИКА ЗАРПЛАТ ЗА НЕДЕЛЮ - {city_name.upper()}', 
                                fontsize=12, fontweight='bold', pad=10)
        ax_salary_trend.set_ylabel('Рубли', fontsize=10)
        ax_salary_trend.tick_params(axis='x', rotation=45)
        ax_salary_trend.grid(True, alpha=0.3, color='lightgray', linestyle='-', linewidth=0.5)
        ax_salary_trend.legend(loc='upper left')
        ax_salary_trend.set_facecolor('white')
        
        # Форматируем оси Y
        ax_salary_trend.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        # Добавляем значения
        for i, (avg, med) in enumerate(zip(avg_salaries, median_salaries)):
            ax_salary_trend.text(i, avg + max(avg_salaries)*0.02, f'{avg:,.0f}', 
                               ha='center', fontsize=9, color='#3498db')
            ax_salary_trend.text(i, med - max(median_salaries)*0.04, f'{med:,.0f}', 
                               ha='center', fontsize=9, color='#2ecc71')
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
        
        # Подсвечиваем вчера (для сравнения)
        yesterday_str = dates['yesterday'].strftime('%d.%m')
        if yesterday_str in dates_activity:
            yesterday_idx = dates_activity.index(yesterday_str)
            bars[yesterday_idx].set_color('#e74c3c')
            bars[yesterday_idx].set_alpha(1.0)
        
        ax_activity.set_title('ВАКАНСИИ ЗА НЕДЕЛЮ', 
                            fontsize=12, fontweight='bold', pad=10)
        ax_activity.set_ylabel('Количество', fontsize=10)
        ax_activity.tick_params(axis='x', rotation=45)
        ax_activity.grid(True, alpha=0.3, axis='y', color='lightgray', linestyle='-', linewidth=0.5)
        ax_activity.set_facecolor('white')
        
        # Добавляем значения
        for i, v in enumerate(daily_activity.values):
            ax_activity.text(i, v + max(daily_activity.values)*0.02, str(v), 
                           ha='center', fontsize=9)
    else:
        ax_activity = fig.add_subplot(gs[1, 0])
        ax_activity.axis('off')
        ax_activity.text(0.5, 0.5, 'Нет данных\nза неделю', 
                        ha='center', va='center', fontsize=12, color='#7f8c8d')
    
    fig.patch.set_facecolor('white')
    plt.tight_layout()
    
    # Сохраняем в буфер
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    
    buf.seek(0)
    return buf

def generate_telegram_text(city_name: str, city_data: pd.DataFrame):
    """Генерация текста дайджеста для Telegram с корректным сравнением"""
    
    # Текущее время по Москве
    moscow_now = datetime.now(MOSCOW_TZ)
    moscow_time_str = moscow_now.strftime('%H:%M')
    
    # Фильтруем зарплатные данные
    city_salary_data = city_data[
        city_data['salary_period_name'].apply(contains_monthly_pattern) & 
        city_data['salary_to_net'].notna()
    ]
    
    # Получаем даты для сравнения (относительно московского времени)
    dates = get_comparison_dates(moscow_now)
    
    # Данные по дням
    data_today = city_data[city_data['published_date'] == dates['today']]
    data_yesterday = city_data[city_data['published_date'] == dates['yesterday']]
    data_day_before = city_data[city_data['published_date'] == dates['day_before']]
    data_week = city_data[city_data['published_date'] >= dates['week_start']]
    data_salary_week = city_salary_data[city_salary_data['published_date'] >= dates['week_start']]
    
    # Зарплатные данные за сегодня
    salary_today = city_salary_data[city_salary_data['published_date'] == dates['today']]
    
    # 1. СРАВНЕНИЕ ПОЛНЫХ ДНЕЙ (вчера vs позавчера)
    if len(data_day_before) > 0 and len(data_yesterday) > 0:
        full_day_growth = len(data_yesterday) - len(data_day_before)
        full_day_growth_pct = (full_day_growth / len(data_day_before)) * 100 if len(data_day_before) > 0 else 0
        
        if full_day_growth > 0:
            comparison_emoji = "📈"
            comparison_text = f"{comparison_emoji} Вчера vs Позавчера: +{full_day_growth:,} ({full_day_growth_pct:+.1f}%)"
        elif full_day_growth < 0:
            comparison_emoji = "📉"
            comparison_text = f"{comparison_emoji} Вчера vs Позавчера: {full_day_growth:,} ({full_day_growth_pct:+.1f}%)"
        else:
            comparison_emoji = "➡️"
            comparison_text = f"{comparison_emoji} Вчера vs Позавчера: без изменений"
    else:
        comparison_text = "⏳ Недостаточно данных для сравнения полных дней"
    
    # 2. СЕГОДНЯ (частичный день)
    today_count = len(data_today)
    today_text = f"📅 Сегодня (на {moscow_time_str}): {today_count:,} вакансий"
    
    # 3. ЗАРПЛАТЫ НА СЕГОДНЯ
    salary_text = ""
    if len(salary_today) > 0:
        avg_salary = salary_today['salary_to_net'].mean()
        median_salary = salary_today['salary_to_net'].median()
        q25 = salary_today['salary_to_net'].quantile(0.25)
        q75 = salary_today['salary_to_net'].quantile(0.75)
        
        salary_text = f"""
💰 Зарплаты сегодня ({len(salary_today):,} вакансий):

✓ Средняя: {safe_format_number(avg_salary)}
✓ Медианная: {safe_format_number(median_salary)}
✓ 25% получают до: {safe_format_number(q25)}
✓ 75% получают до: {safe_format_number(q75)}
✓ Вилка: {safe_format_number(q75 - q25)}
"""
    else:
        salary_text = "💰 Сегодня нет данных о зарплатах"
    
    # 4. ТОП РАБОТОДАТЕЛИ СЕГОДНЯ
    employers_text = ""
    top_employers_today = data_today['employer'].value_counts().head(3)
    if len(top_employers_today) > 0:
        employers_text = "🏢 ТОП работодателей сегодня:\n\n"
        for i, (employer, count) in enumerate(top_employers_today.items(), 1):
            employer_short = employer[:25] + '...' if len(employer) > 25 else employer
            employers_text += f"{i}. {employer_short} - {count:,} вакансий\n"
    else:
        employers_text = "🏢 Сегодня нет данных о работодателях"
    
    # 5. ОБЩАЯ СТАТИСТИКА ПО ГОРОДУ
    general_stats = f"""
📊 Сводка по {city_name}:

✓ Всего вакансий: {len(city_data):,}
✓ С зарплатой 'за месяц': {len(city_salary_data):,}
✓ Средняя зарплата: {safe_format_number(city_salary_data['salary_to_net'].mean())}
✓ Период данных: {city_data['published_date'].min()} - {dates['today']}
"""
    
    # Формируем финальное сообщение
    telegram_text = f"""📊 Аналитика рынка вакансий - {city_name}

{comparison_text}
{today_text}

📈 За неделю: {len(data_week):,} вакансий
💰 С зарплатой за неделю: {len(data_salary_week):,}

{salary_text}

{employers_text}

{general_stats}

⏰ Обновлено: {moscow_time_str} МСК
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
    
    # Проходим по каждому городу
    for city_slug, city_info in CITIES.items():
        print(f"\n📍 Обработка города: {city_info['name']} ({city_slug})")
        
        # Фильтруем данные по городу
        city_data = df[df['city_slug'] == city_slug]
        
        if len(city_data) == 0:
            print(f"⚠️ Нет данных для города {city_info['name']}")
            continue
        
        try:
            # Получаем текущее московское время для дайджеста
            moscow_now = datetime.now(MOSCOW_TZ)
            
            # Создаем изображение
            print(f"🎨 Генерируем изображение для {city_info['name']}...")
            image_buf = create_digest_image(city_info['name'], city_data, moscow_now)
            
            # Генерируем текст
            print(f"📝 Генерируем текст для {city_info['name']}...")
            text = generate_telegram_text(city_info['name'], city_data)
            
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
