import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from io import BytesIO
from supabase import create_client
import asyncio
from telegram import Bot
import pytz
import calendar
from typing import Dict, List, Tuple, Optional
import re
import warnings
warnings.filterwarnings('ignore')

# Константы
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Конфигурация городов
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

def normalize_text(text):
    """Нормализация текста: замена всех пробелов и приведение к нижнему регистру"""
    if pd.isna(text):
        return text
    text = str(text)
    # Заменяем все виды пробелов на обычные
    text = re.sub(r'[ \t\n\r\f\v\xa0\u2000-\u200f]+', ' ', text)
    return text.strip().lower()

def get_previous_month_range(report_date: datetime) -> Tuple[datetime, datetime]:
    """Получить начало и конец ПРЕДЫДУЩЕГО месяца для отчета"""
    # Определяем предыдущий месяц
    if report_date.month == 1:
        prev_month_date = report_date.replace(year=report_date.year - 1, month=12, day=1)
    else:
        prev_month_date = report_date.replace(month=report_date.month - 1, day=1)
    
    # Начало месяца
    month_start = prev_month_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Конец месяца
    if prev_month_date.month == 12:
        next_month = prev_month_date.replace(year=prev_month_date.year + 1, month=1, day=1)
    else:
        next_month = prev_month_date.replace(month=prev_month_date.month + 1, day=1)
    
    month_end = next_month - timedelta(seconds=1)
    
    return month_start, month_end

def get_month_before_previous_range(report_date: datetime) -> Tuple[datetime, datetime]:
    """Получить начало и конец месяца, предшествующего предыдущему (для сравнения)"""
    # На два месяца назад
    if report_date.month == 1:
        two_months_ago = report_date.replace(year=report_date.year - 1, month=11, day=1)
    elif report_date.month == 2:
        two_months_ago = report_date.replace(year=report_date.year - 1, month=12, day=1)
    else:
        two_months_ago = report_date.replace(month=report_date.month - 2, day=1)
    
    month_start = two_months_ago.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if two_months_ago.month == 12:
        next_month = two_months_ago.replace(year=two_months_ago.year + 1, month=1, day=1)
    else:
        next_month = two_months_ago.replace(month=two_months_ago.month + 1, day=1)
    
    month_end = next_month - timedelta(seconds=1)
    
    return month_start, month_end

def calculate_ema(series: pd.Series, span: int = 7) -> pd.Series:
    """Вычисление экспоненциального скользящего среднего"""
    return series.ewm(span=span, adjust=False).mean()

def analyze_trend_from_ema(ema_series: pd.Series) -> Dict:
    """Анализ тренда на основе EMA"""
    if len(ema_series) < 2:
        return {}
    
    trend_start = ema_series.iloc[0]
    trend_end = ema_series.iloc[-1]
    trend_change = trend_end - trend_start
    trend_pct = (trend_change / trend_start * 100) if trend_start > 0 else 0
    
    # Определяем силу тренда
    if abs(trend_pct) > 5:
        strength = "сильный"
    elif abs(trend_pct) > 2:
        strength = "умеренный"
    else:
        strength = "слабый"
    
    # Определяем направление
    if trend_pct > 1:
        direction = "восходящий"
        emoji = "📈"
    elif trend_pct < -1:
        direction = "нисходящий"
        emoji = "📉"
    else:
        direction = "боковой"
        emoji = "➡️"
    
    return {
        'start': trend_start,
        'end': trend_end,
        'change': trend_change,
        'pct': trend_pct,
        'direction': direction,
        'strength': strength,
        'emoji': emoji
    }

def load_monthly_data_from_supabase(month_start: datetime, month_end: datetime):
    """Загрузка данных за месяц из Supabase"""
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("Не найдены переменные окружения SUPABASE_URL или SUPABASE_KEY")
    
    supabase_client = create_client(supabase_url, supabase_key)
    
    # Загружаем данные за нужный месяц
    all_data = []
    page = 0
    limit = 1000

    while True:
        response = supabase_client.table("vacancies") \
            .select("*") \
            .gte('published_at', month_start.isoformat()) \
            .lte('published_at', month_end.isoformat()) \
            .range(page * limit, (page + 1) * limit - 1) \
            .execute()

        if not response.data:
            break

        all_data.extend(response.data)
        page += 1
        print(f"  Загружено страниц: {page}, всего строк: {len(all_data)}")

    df = pd.DataFrame(all_data)
    
    if len(df) == 0:
        print(f"  ⚠️ Нет данных за период {month_start.strftime('%d.%m.%Y')} - {month_end.strftime('%d.%m.%Y')}")
        return pd.DataFrame()
    
    print(f"  ✅ Итого загружено {len(df)} строк")
    
    # Преобразуем даты
    if 'published_at' in df.columns:
        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
        df['published_at_moscow'] = df['published_at'].dt.tz_convert(MOSCOW_TZ)
        df['published_date'] = df['published_at_moscow'].dt.date
        df['published_day'] = df['published_at_moscow'].dt.day
        df['published_week'] = df['published_at_moscow'].dt.isocalendar().week
        df['published_weekday'] = df['published_at_moscow'].dt.day_name()
    
    # Нормализуем текстовые поля для сравнения
    text_columns = ['salary_period_name', 'schedule_name', 'experience_name', 'employment_form_name']
    for col in text_columns:
        if col in df.columns:
            df[f'{col}_normalized'] = df[col].apply(normalize_text)
    
    return df

def is_monthly_salary(text):
    """Определяет, является ли зарплата месячной"""
    if pd.isna(text):
        return False
    
    text_normalized = normalize_text(text)
    
    # Проверяем различные варианты написания
    monthly_keywords = ['месяц', 'month', 'мес']
    return any(keyword in text_normalized for keyword in monthly_keywords)

def analyze_monthly_metrics(city_data: pd.DataFrame, prev_month_data: pd.DataFrame = None) -> Dict:
    """Анализ основных метрик за месяц"""
    
    metrics = {}
    
    # 1. Базовые метрики
    metrics['total_vacancies'] = len(city_data)
    
    # 2. Зарплатные метрики
    # Используем нормализованное поле для фильтрации
    monthly_salary_mask = city_data['salary_period_name_normalized'].apply(is_monthly_salary)
    monthly_salary_data = city_data[monthly_salary_mask & city_data['salary_to_net'].notna()]
    
    metrics['with_monthly_salary'] = len(monthly_salary_data)
    metrics['salary_percentage'] = (metrics['with_monthly_salary'] / metrics['total_vacancies'] * 100) if metrics['total_vacancies'] > 0 else 0
    
    if len(monthly_salary_data) > 0:
        metrics['avg_salary'] = monthly_salary_data['salary_to_net'].mean()
        metrics['median_salary'] = monthly_salary_data['salary_to_net'].median()
        metrics['salary_std'] = monthly_salary_data['salary_to_net'].std()
        
        # Квартили
        metrics['q25'] = monthly_salary_data['salary_to_net'].quantile(0.25)
        metrics['q75'] = monthly_salary_data['salary_to_net'].quantile(0.75)
        metrics['q90'] = monthly_salary_data['salary_to_net'].quantile(0.90)
        metrics['q10'] = monthly_salary_data['salary_to_net'].quantile(0.10)
        
        # Зарплатные вилки (используем salary_from_net если есть)
        if 'salary_from_net' in monthly_salary_data.columns:
            salary_with_range = monthly_salary_data[monthly_salary_data['salary_from_net'].notna()]
            if len(salary_with_range) > 0:
                metrics['avg_salary_range'] = (salary_with_range['salary_to_net'] - salary_with_range['salary_from_net']).mean()
                metrics['vacancies_with_range'] = len(salary_with_range)
            else:
                metrics['avg_salary_range'] = 0
                metrics['vacancies_with_range'] = 0
        else:
            metrics['avg_salary_range'] = 0
            metrics['vacancies_with_range'] = 0
        
        # Анализ динамики зарплат по дням для EMA
        if 'published_day' in monthly_salary_data.columns:
            daily_avg_salary = monthly_salary_data.groupby('published_day')['salary_to_net'].mean()
            if len(daily_avg_salary) >= 7:
                ema_series = calculate_ema(daily_avg_salary.sort_index(), span=7)
                trend_analysis = analyze_trend_from_ema(ema_series)
                metrics['trend_analysis'] = trend_analysis
    
    # 3. Анализ графиков работы
    if 'schedule_name_normalized' in city_data.columns:
        schedule_counts = city_data['schedule_name_normalized'].value_counts()
        metrics['top_schedules'] = schedule_counts.head(3).to_dict()
        metrics['total_schedules'] = len(schedule_counts)
    
    # 4. Анализ дней недели
    if 'published_weekday' in city_data.columns:
        weekday_counts = city_data['published_weekday'].value_counts()
        metrics['top_weekday'] = weekday_counts.index[0] if len(weekday_counts) > 0 else None
        metrics['weekday_counts'] = weekday_counts.to_dict()
    
    # 5. ТОП работодателей
    if 'employer' in city_data.columns:
        # Очищаем названия работодателей
        city_data['employer_clean'] = city_data['employer'].str.strip().fillna('Не указан')
        employer_counts = city_data['employer_clean'].value_counts().head(5)
        metrics['top_employers_count'] = employer_counts.to_dict()
        
        # ТОП работодателей по зарплате (только те, у кого > 3 вакансий)
        if len(monthly_salary_data) > 0:
            if 'employer_clean' in monthly_salary_data.columns:
                employer_avg_salary = monthly_salary_data.groupby('employer_clean').agg({
                    'salary_to_net': ['mean', 'count']
                }).round(0)
                employer_avg_salary.columns = ['avg_salary', 'vacancy_count']
                employer_avg_salary = employer_avg_salary[employer_avg_salary['vacancy_count'] >= 3]
                if len(employer_avg_salary) > 0:
                    metrics['top_employers_salary'] = employer_avg_salary.nlargest(5, 'avg_salary')['avg_salary'].to_dict()
    
    # 6. Сравнение с предыдущим месяцем (если есть данные)
    if prev_month_data is not None and len(prev_month_data) > 0:
        prev_month_metrics = analyze_monthly_metrics(prev_month_data)
        
        metrics['prev_month_total'] = prev_month_metrics.get('total_vacancies', 0)
        metrics['total_growth'] = metrics['total_vacancies'] - metrics['prev_month_total']
        metrics['total_growth_pct'] = (metrics['total_growth'] / metrics['prev_month_total'] * 100) if metrics['prev_month_total'] > 0 else 0
        
        if 'avg_salary' in metrics and 'avg_salary' in prev_month_metrics:
            metrics['salary_growth'] = metrics['avg_salary'] - prev_month_metrics['avg_salary']
            metrics['salary_growth_pct'] = (metrics['salary_growth'] / prev_month_metrics['avg_salary'] * 100) if prev_month_metrics['avg_salary'] > 0 else 0
    
    return metrics

def create_monthly_report_image(city_name: str, city_data: pd.DataFrame, metrics: Dict) -> BytesIO:
    """Создание изображения месячного отчета с EMA"""
    
    # Настройка стилей
    plt.style.use('seaborn-v0_8-whitegrid')
    plt.rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['axes.unicode_minus'] = False
    
    # Создаем фигуру с 6 графиками (3x2)
    fig = plt.figure(figsize=(14, 16), facecolor='white')
    
    # 1. РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ (верхний левый)
    ax1 = plt.subplot(3, 2, 1)
    
    # Получаем месячные зарплаты (уже отфильтрованные)
    monthly_mask = city_data['salary_period_name_normalized'].apply(is_monthly_salary)
    monthly_salary_data = city_data[monthly_mask & city_data['salary_to_net'].notna()]
    
    if len(monthly_salary_data) > 0:
        salaries = monthly_salary_data['salary_to_net']
        # Убираем выбросы для лучшего отображения
        if len(salaries) > 0:
            q1, q3 = salaries.quantile(0.25), salaries.quantile(0.75)
            iqr = q3 - q1
            lower_bound, upper_bound = q1 - 1.5*iqr, q3 + 1.5*iqr
            filtered_salaries = salaries[(salaries >= lower_bound) & (salaries <= upper_bound)]
            
            if len(filtered_salaries) > 0:
                ax1.hist(filtered_salaries, bins=15, color='#3498db', edgecolor='white', alpha=0.7)
                
                if 'avg_salary' in metrics:
                    ax1.axvline(metrics['avg_salary'], color='red', linestyle='--', 
                               linewidth=2, label=f'Средняя: {metrics["avg_salary"]:,.0f} ₽')
                if 'median_salary' in metrics:
                    ax1.axvline(metrics['median_salary'], color='green', linestyle='--',
                               linewidth=2, label=f'Медиана: {metrics["median_salary"]:,.0f} ₽')
                
                ax1.set_title('РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ ЗА МЕСЯЦ', fontsize=11, fontweight='bold')
                ax1.set_xlabel('Зарплата, ₽', fontsize=9)
                ax1.set_ylabel('Количество вакансий', fontsize=9)
                ax1.legend(fontsize=8)
                ax1.grid(True, alpha=0.3)
                ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
            else:
                ax1.text(0.5, 0.5, 'Недостаточно данных\nдля гистограммы', 
                        ha='center', va='center', fontsize=10, color='gray')
                ax1.set_title('РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ ЗА МЕСЯЦ', fontsize=11, fontweight='bold')
        else:
            ax1.text(0.5, 0.5, 'Нет данных\nо зарплатах', ha='center', va='center', 
                    fontsize=10, color='gray')
            ax1.set_title('РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ ЗА МЕСЯЦ', fontsize=11, fontweight='bold')
    else:
        ax1.text(0.5, 0.5, 'Нет данных\nо зарплатах за месяц', ha='center', va='center', 
                fontsize=10, color='gray')
        ax1.set_title('РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ ЗА МЕСЯЦ', fontsize=11, fontweight='bold')
    
    # 2. АКТИВНОСТЬ ПО НЕДЕЛЯМ (верхний правый)
    ax2 = plt.subplot(3, 2, 2)
    if 'published_week' in city_data.columns and len(city_data) > 0:
        weekly_counts = city_data.groupby('published_week').size()
        if len(weekly_counts) > 0:
            weeks = [f'Нед. {w}' for w in weekly_counts.index]
            bars = ax2.bar(weeks, weekly_counts.values, color='#9b59b6', alpha=0.7)
            ax2.set_title('АКТИВНОСТЬ ПО НЕДЕЛЯМ', fontsize=11, fontweight='bold')
            ax2.set_ylabel('Количество вакансий', fontsize=9)
            ax2.tick_params(axis='x', rotation=45)
            
            for bar in bars:
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                        f'{int(height)}', ha='center', va='bottom', fontsize=8)
        else:
            ax2.text(0.5, 0.5, 'Нет данных\nпо неделям', ha='center', va='center',
                    fontsize=10, color='gray')
            ax2.set_title('АКТИВНОСТЬ ПО НЕДЕЛЯМ', fontsize=11, fontweight='bold')
    else:
        ax2.text(0.5, 0.5, 'Нет данных\nпо неделям', ha='center', va='center',
                fontsize=10, color='gray')
        ax2.set_title('АКТИВНОСТЬ ПО НЕДЕЛЯМ', fontsize=11, fontweight='bold')
    
    # 3. ГРАФИКИ РАБОТЫ (средний левый)
    ax3 = plt.subplot(3, 2, 3)
    if 'schedule_name_normalized' in city_data.columns and len(city_data) > 0:
        schedule_data = city_data[city_data['schedule_name_normalized'].notna()]
        if len(schedule_data) > 0:
            schedule_counts = schedule_data['schedule_name_normalized'].value_counts().head(5)
            if len(schedule_counts) > 0:
                # Восстанавливаем оригинальные названия для отображения
                original_names = {}
                for idx in schedule_counts.index:
                    original = city_data[city_data['schedule_name_normalized'] == idx]['schedule_name'].iloc[0] if 'schedule_name' in city_data.columns else idx
                    original_names[idx] = original
                
                colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6']
                wedges, texts, autotexts = ax3.pie(schedule_counts.values, labels=None,
                                                  autopct='%1.1f%%', startangle=90,
                                                  colors=colors[:len(schedule_counts)])
                ax3.set_title('РАСПРЕДЕЛЕНИЕ ПО ГРАФИКАМ РАБОТЫ', fontsize=11, fontweight='bold')
                
                legend_labels = [f'{original_names.get(label, label)} ({count})' 
                               for label, count in zip(schedule_counts.index, schedule_counts.values)]
                ax3.legend(wedges, legend_labels, title="Графики", loc="center left",
                          bbox_to_anchor=(1, 0, 0.5, 1), fontsize=8)
            else:
                ax3.text(0.5, 0.5, 'Нет данных\nо графиках работы', ha='center', va='center',
                        fontsize=10, color='gray')
                ax3.set_title('РАСПРЕДЕЛЕНИЕ ПО ГРАФИКАМ РАБОТЫ', fontsize=11, fontweight='bold')
        else:
            ax3.text(0.5, 0.5, 'Нет данных\nо графиках работы', ha='center', va='center',
                    fontsize=10, color='gray')
            ax3.set_title('РАСПРЕДЕЛЕНИЕ ПО ГРАФИКАМ РАБОТЫ', fontsize=11, fontweight='bold')
    else:
        ax3.text(0.5, 0.5, 'Нет данных\nо графиках работы', ha='center', va='center',
                fontsize=10, color='gray')
        ax3.set_title('РАСПРЕДЕЛЕНИЕ ПО ГРАФИКАМ РАБОТЫ', fontsize=11, fontweight='bold')
    
    # 4. ДНИ НЕДЕЛИ (средний правый)
    ax4 = plt.subplot(3, 2, 4)
    if 'published_weekday' in city_data.columns and len(city_data) > 0:
        # Порядок дней недели
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        days_rus = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
        
        weekday_counts = city_data['published_weekday'].value_counts()
        # Приводим к правильному порядку
        ordered_counts = [weekday_counts.get(day, 0) for day in days_order]
        
        if sum(ordered_counts) > 0:
            bars = ax4.bar(days_rus, ordered_counts, color='#1abc9c', alpha=0.7)
            ax4.set_title('АКТИВНОСТЬ ПО ДНЯМ НЕДЕЛИ', fontsize=11, fontweight='bold')
            ax4.set_ylabel('Количество вакансий', fontsize=9)
            
            # Подсветка пикового дня
            if len(ordered_counts) > 0:
                max_idx = np.argmax(ordered_counts)
                bars[max_idx].set_color('#e74c3c')
                bars[max_idx].set_alpha(1.0)
        else:
            ax4.text(0.5, 0.5, 'Нет данных\nпо дням недели', ha='center', va='center',
                    fontsize=10, color='gray')
            ax4.set_title('АКТИВНОСТЬ ПО ДНЯМ НЕДЕЛИ', fontsize=11, fontweight='bold')
    else:
        ax4.text(0.5, 0.5, 'Нет данных\nпо дням недели', ha='center', va='center',
                fontsize=10, color='gray')
        ax4.set_title('АКТИВНОСТЬ ПО ДНЯМ НЕДЕЛИ', fontsize=11, fontweight='bold')
    
    # 5. ТОП РАБОТОДАТЕЛИ ПО КОЛИЧЕСТВУ (нижний левый)
    ax5 = plt.subplot(3, 2, 5)
    if 'employer_clean' in city_data.columns and len(city_data) > 0:
        city_data_clean = city_data[city_data['employer_clean'] != 'Не указан']
        if len(city_data_clean) > 0:
            top_employers = city_data_clean['employer_clean'].value_counts().head(5)
            if len(top_employers) > 0:
                employers_short = [e[:20] + '...' if len(e) > 20 else e for e in top_employers.index]
                y_pos = np.arange(len(employers_short))
                bars = ax5.barh(y_pos, top_employers.values, color='#3498db', alpha=0.7)
                ax5.set_yticks(y_pos)
                ax5.set_yticklabels(employers_short, fontsize=8)
                ax5.invert_yaxis()
                ax5.set_title('ТОП-5 РАБОТОДАТЕЛЕЙ', fontsize=11, fontweight='bold')
                ax5.set_xlabel('Количество вакансий', fontsize=9)
                
                for i, v in enumerate(top_employers.values):
                    ax5.text(v + 0.5, i, str(v), va='center', fontsize=8)
            else:
                ax5.text(0.5, 0.5, 'Нет данных\nо работодателях', ha='center', va='center',
                        fontsize=10, color='gray')
                ax5.set_title('ТОП-5 РАБОТОДАТЕЛЕЙ', fontsize=11, fontweight='bold')
        else:
            ax5.text(0.5, 0.5, 'Нет данных\nо работодателях', ha='center', va='center',
                    fontsize=10, color='gray')
            ax5.set_title('ТОП-5 РАБОТОДАТЕЛЕЙ', fontsize=11, fontweight='bold')
    else:
        ax5.text(0.5, 0.5, 'Нет данных\nо работодателях', ha='center', va='center',
                fontsize=10, color='gray')
        ax5.set_title('ТОП-5 РАБОТОДАТЕЛЕЙ', fontsize=11, fontweight='bold')
    
    # 6. ДИНАМИКА ЗАРПЛАТ С EMA (нижний правый)
    ax6 = plt.subplot(3, 2, 6)
    if 'published_day' in city_data.columns and len(monthly_salary_data) > 0:
        daily_avg_salary = monthly_salary_data.groupby('published_day')['salary_to_net'].mean()
        daily_median_salary = monthly_salary_data.groupby('published_day')['salary_to_net'].median()
        
        if len(daily_avg_salary) > 0:
            # Сортируем по дням
            daily_avg_salary = daily_avg_salary.sort_index()
            daily_median_salary = daily_median_salary.sort_index()
            
            days = list(range(1, 32))
            avg_salaries = [daily_avg_salary.get(day, np.nan) for day in days]
            median_salaries = [daily_median_salary.get(day, np.nan) for day in days]
            
            # Убираем NaN для отображения
            valid_days = [day for day, sal in zip(days, avg_salaries) if not np.isnan(sal)]
            valid_avg = [sal for sal in avg_salaries if not np.isnan(sal)]
            valid_median = [median_salaries[day-1] for day in valid_days]
            
            if len(valid_days) > 1:
                # Рисуем основные линии
                avg_line, = ax6.plot(valid_days, valid_avg, 'o-', color='#3498db', 
                                   label='Средняя за день', linewidth=2, markersize=4, alpha=0.7)
                median_line, = ax6.plot(valid_days, valid_median, 's--', color='#2ecc71', 
                                      label='Медиана за день', linewidth=1.5, markersize=3, alpha=0.7)
                
                # EMA для средней зарплаты
                if len(valid_avg) >= 3:
                    ema_series = calculate_ema(pd.Series(valid_avg, index=valid_days), 
                                              span=min(7, len(valid_avg)))
                    ema_line, = ax6.plot(valid_days, ema_series.values, color='#e74c3c', 
                                       linewidth=3, label=f'Тренд (EMA{min(7, len(valid_avg))})', 
                                       alpha=0.8, zorder=5)
                    
                    # Анализ тренда
                    trend_info = analyze_trend_from_ema(ema_series)
                    
                    if trend_info:
                        trend_text = f"{trend_info['emoji']} {trend_info['direction'].upper()}\n{trend_info['strength']} {trend_info['pct']:+.1f}%"
                        
                        ax6.annotate(trend_text, xy=(0.98, 0.95), xycoords='axes fraction',
                                   fontsize=9, color='#e74c3c', fontweight='bold',
                                   ha='right', va='top',
                                   bbox=dict(boxstyle="round,pad=0.3", 
                                            facecolor='white', 
                                            edgecolor='#e74c3c',
                                            alpha=0.9))
                
                ax6.set_title('ДИНАМИКА ЗАРПЛАТ С АНАЛИЗОМ ТРЕНДА', fontsize=11, fontweight='bold')
                ax6.set_xlabel('День месяца', fontsize=9)
                ax6.set_ylabel('Зарплата, ₽', fontsize=9)
                ax6.legend(fontsize=8, loc='lower center', bbox_to_anchor=(0.5, -0.35), 
                          ncol=2, framealpha=0.9)
                ax6.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
                ax6.set_xticks(range(1, 32, 5))
                ax6.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
                
                # Автоматическое масштабирование
                all_values = valid_avg + valid_median
                if len(all_values) > 0:
                    y_min, y_max = min(all_values), max(all_values)
                    y_range = y_max - y_min
                    ax6.set_ylim(y_min - y_range*0.1, y_max + y_range*0.1)
            else:
                ax6.text(0.5, 0.5, 'Недостаточно дней\nдля анализа тренда', 
                        ha='center', va='center', fontsize=10, color='gray')
                ax6.set_title('ДИНАМИКА ЗАРПЛАТ С АНАЛИЗОМ ТРЕНДА', fontsize=11, fontweight='bold')
        else:
            ax6.text(0.5, 0.5, 'Нет данных\nо зарплатах по дням', 
                    ha='center', va='center', fontsize=10, color='gray')
            ax6.set_title('ДИНАМИКА ЗАРПЛАТ С АНАЛИЗОМ ТЕНДЕНЦИЙ', fontsize=11, fontweight='bold')
    else:
        ax6.text(0.5, 0.5, 'Нет данных\nдля анализа тренда', 
                ha='center', va='center', fontsize=10, color='gray')
        ax6.set_title('ДИНАМИКА ЗАРПЛАТ С АНАЛИЗОМ ТРЕНДА', fontsize=11, fontweight='bold')
    
    # Общий заголовок
    month_name_ru = {
        1: "ЯНВАРЬ", 2: "ФЕВРАЛЬ", 3: "МАРТ", 4: "АПРЕЛЬ",
        5: "МАЙ", 6: "ИЮНЬ", 7: "ИЮЛЬ", 8: "АВГУСТ",
        9: "СЕНТЯБРЬ", 10: "ОКТЯБРЬ", 11: "НОЯБРЬ", 12: "ДЕКАБРЬ"
    }
    
    # Месяц отчета (предыдущий месяц)
    report_month = datetime.now(MOSCOW_TZ)
    if report_month.month == 1:
        report_month = report_month.replace(year=report_month.year - 1, month=12)
    else:
        report_month = report_month.replace(month=report_month.month - 1)
    
    month_name = month_name_ru.get(report_month.month, report_month.strftime('%B').upper())
    year = report_month.year
    
    fig.suptitle(f'МЕСЯЧНЫЙ ОТЧЕТ: {city_name.upper()} - {month_name} {year}', 
                fontsize=14, fontweight='bold', y=0.98)
    
    plt.tight_layout(rect=[0, 0.02, 1, 0.96])
    
    # Сохраняем в буфер
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none', 
                pad_inches=0.1)
    plt.close()
    
    buf.seek(0)
    return buf

def generate_monthly_telegram_text(city_name: str, metrics: Dict, month_start: datetime) -> str:
    """Генерация текста месячного отчета для Telegram"""
    
    # Название месяца на русском
    month_names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }
    
    # Месяц отчета (предыдущий месяц)
    report_month = month_start.month
    year = month_start.year
    
    month_name = month_names.get(report_month, "Неизвестный месяц")
    
    # Форматирование
    def format_num(num):
        return f"{num:,.0f}".replace(",", " ")
    
    def format_money(num):
        return f"{format_num(num)} ₽" if num else "нет данных"
    
    def format_pct(num):
        return f"{num:+.1f}%" if num != 0 else "0%"
    
    # Строим сообщение
    message = f"📊 *МЕСЯЧНЫЙ ОТЧЕТ: {city_name.upper()} - {month_name} {year}*\n\n"
    
    # ОСНОВНЫЕ ПОКАЗАТЕЛИ
    message += "📈 *ОСНОВНЫЕ ПОКАЗАТЕЛИ:*\n"
    message += f"• Всего вакансий: *{format_num(metrics.get('total_vacancies', 0))}*\n"
    message += f"• С зарплатой 'за месяц': *{format_num(metrics.get('with_monthly_salary', 0))}* "
    message += f"({metrics.get('salary_percentage', 0):.1f}%)\n"
    
    if 'avg_salary' in metrics:
        message += f"• Средняя зарплата: *{format_money(metrics['avg_salary'])}*\n"
        message += f"• Медианная зарплата: *{format_money(metrics['median_salary'])}*\n"
        
        if 'q25' in metrics and 'q75' in metrics:
            message += f"• 25% получают до: *{format_money(metrics['q25'])}*\n"
            message += f"• 75% получают до: *{format_money(metrics['q75'])}*\n"
        
        if 'q90' in metrics:
            message += f"• ТОП-10%: от *{format_money(metrics['q90'])}*\n"
    
    # ТРЕНД EMA
    if 'trend_analysis' in metrics:
        trend = metrics['trend_analysis']
        message += f"\n{trend['emoji']} *ТРЕНД ЗАРПЛАТ (EMA):*\n"
        message += f"• Направление: *{trend['direction']}*\n"
        message += f"• Изменение: *{format_pct(trend['pct'])}*\n"
        if 'start' in trend and 'end' in trend:
            message += f"• С *{format_money(trend['start'])}* до *{format_money(trend['end'])}*\n"
        message += f"• Сила тренда: *{trend['strength']}*\n"
    
    # СРАВНЕНИЕ С ПРЕДЫДУЩИМ МЕСЯЦЕМ
    if 'total_growth' in metrics:
        growth_emoji = "📈" if metrics['total_growth'] > 0 else "📉" if metrics['total_growth'] < 0 else "➡️"
        message += f"\n{growth_emoji} *СРАВНЕНИЕ С ПРЕДЫДУЩИМ МЕСЯЦЕМ:*\n"
        message += f"• Вакансий: *{format_pct(metrics['total_growth_pct'])}* "
        message += f"({format_num(metrics['total_growth'])})\n"
        
        if 'salary_growth' in metrics:
            salary_emoji = "💰📈" if metrics['salary_growth'] > 0 else "💰📉" if metrics['salary_growth'] < 0 else "💰➡️"
            message += f"• {salary_emoji} Зарплата: *{format_pct(metrics['salary_growth_pct'])}* "
            message += f"({format_money(metrics['salary_growth'])})\n"
    
    # ГРАФИКИ РАБОТЫ
    message += "\n⏰ *ПОПУЛЯРНЫЕ ГРАФИКИ:*\n"
    if 'top_schedules' in metrics and metrics['top_schedules']:
        for schedule, count in list(metrics['top_schedules'].items())[:3]:
            pct = (count / metrics['total_vacancies'] * 100) if metrics['total_vacancies'] > 0 else 0
            # Восстанавливаем оригинальное название
            schedule_display = schedule.title() if schedule else "Не указан"
            message += f"• {schedule_display}: *{count}* ({pct:.1f}%)\n"
    else:
        message += "• Нет данных о графиках\n"
    
    # ДНИ НЕДЕЛИ
    message += "\n📅 *АКТИВНОСТЬ ПО ДНЯМ:*\n"
    if 'top_weekday' in metrics and metrics['top_weekday']:
        weekdays_ru = {
            'Monday': 'Понедельник', 'Tuesday': 'Вторник',
            'Wednesday': 'Среда', 'Thursday': 'Четверг',
            'Friday': 'Пятница', 'Saturday': 'Суббота',
            'Sunday': 'Воскресенье'
        }
        top_day = weekdays_ru.get(metrics['top_weekday'], metrics['top_weekday'])
        message += f"• Больше всего в *{top_day}*\n"
        
        if 'weekday_counts' in metrics:
            total_days = sum(metrics['weekday_counts'].values())
            avg_per_day = total_days / 7 if total_days > 0 else 0
            message += f"• В среднем: *{avg_per_day:.1f}* вакансий/день\n"
    
    # ТОП РАБОТОДАТЕЛИ
    message += "\n🏢 *ТОП РАБОТОДАТЕЛИ:*\n"
    if 'top_employers_count' in metrics and metrics['top_employers_count']:
        message += "*По количеству вакансий:*\n"
        for i, (employer, count) in enumerate(metrics['top_employers_count'].items(), 1):
            employer_display = employer if employer != 'Не указан' else 'Не указан работодатель'
            message += f"{i}. {employer_display}: *{count}*\n"
    
    if 'top_employers_salary' in metrics and metrics['top_employers_salary']:
        message += "\n*По средней зарплате:*\n"
        for i, (employer, salary) in enumerate(metrics['top_employers_salary'].items(), 1):
            employer_display = employer if employer != 'Не указан' else 'Не указан работодатель'
            message += f"{i}. {employer_display}: *{format_money(salary)}*\n"
    
    # ЗАРПЛАТНЫЕ ВИЛКИ
    if 'vacancies_with_range' in metrics and metrics['vacancies_with_range'] > 0:
        message += f"\n💰 *ЗАРПЛАТНЫЕ ВИЛКИ:*\n"
        message += f"• Вакансий с вилкой: *{format_num(metrics['vacancies_with_range'])}*\n"
        message += f"• Средняя вилка: *{format_money(metrics.get('avg_salary_range', 0))}*\n"
    
    # ВРЕМЯ ОБНОВЛЕНИЯ
    moscow_now = datetime.now(MOSCOW_TZ)
    update_time = moscow_now.strftime('%d.%m.%Y %H:%M')
    message += f"\n⏰ *Отчет сгенерирован:* {update_time} МСК\n"
    
    return message

async def send_monthly_report(bot_token: str, channel: str, image_buf: BytesIO, text: str):
    """Отправка месячного отчета в Telegram канал"""
    bot = Bot(token=bot_token)
    
    image_buf.seek(0)
    await bot.send_photo(chat_id=channel, photo=image_buf, caption=text, parse_mode='Markdown')
    
    print(f"  ✅ Отчет отправлен в {channel}")

async def main_monthly_report():
    """Основная функция для генерации месячного отчета"""
    print("🚀 ЗАПУСК МЕСЯЧНОГО ОТЧЕТА")
    print("="*50)
    
    # Текущая дата
    current_date = datetime.now(MOSCOW_TZ)
    print(f"📅 Текущая дата: {current_date.strftime('%d.%m.%Y %H:%M')}")
    
    # ОТЧЕТ ЗА ПРЕДЫДУЩИЙ МЕСЯЦ
    print("\n📊 ОТЧЕТНЫЙ ПЕРИОД (ПРЕДЫДУЩИЙ МЕСЯЦ):")
    month_start, month_end = get_previous_month_range(current_date)
    print(f"   Начало: {month_start.strftime('%d.%m.%Y %H:%M:%S')}")
    print(f"   Конец:  {month_end.strftime('%d.%m.%Y %H:%M:%S')}")
    
    # ДАННЫЕ ДЛЯ СРАВНЕНИЯ (месяц перед предыдущим)
    print("\n📊 ПЕРИОД ДЛЯ СРАВНЕНИЯ (2 МЕСЯЦА НАЗАД):")
    prev_month_start, prev_month_end = get_month_before_previous_range(current_date)
    print(f"   Начало: {prev_month_start.strftime('%d.%m.%Y %H:%M:%S')}")
    print(f"   Конец:  {prev_month_end.strftime('%d.%m.%Y %H:%M:%S')}")
    
    # Загружаем данные
    print("\n📦 ЗАГРУЗКА ДАННЫХ ИЗ SUPABASE...")
    
    # Данные за отчетный месяц (предыдущий месяц)
    df_current = load_monthly_data_from_supabase(month_start, month_end)
    
    if len(df_current) == 0:
        print("❌ Нет данных за отчетный период. Отчет не может быть сгенерирован.")
        return
    
    # Данные для сравнения (месяц перед предыдущим)
    df_previous = None
    try:
        df_previous = load_monthly_data_from_supabase(prev_month_start, prev_month_end)
        if len(df_previous) == 0:
            print("ℹ️ Нет данных за период для сравнения")
            df_previous = None
    except Exception as e:
        print(f"⚠️ Ошибка при загрузке данных для сравнения: {e}")
        df_previous = None
    
    # Получаем токен бота
    bot_token = os.environ.get("TG_BOT_TOKEN")
    if not bot_token:
        raise ValueError("❌ Не найдена переменная окружения TG_BOT_TOKEN")
    
    # Статистика по городам
    print(f"\n🏙️ СТАТИСТИКА ПО ГОРОДАМ (отчетный период):")
    if 'city_slug' in df_current.columns:
        city_stats = df_current['city_slug'].value_counts()
        for city_slug, count in city_stats.items():
            city_name = CITIES.get(city_slug, {}).get('name', city_slug)
            print(f"  {city_name}: {count} вакансий")
    
    # Обрабатываем каждый город
    successful_reports = 0
    failed_reports = 0
    
    for city_slug, city_info in CITIES.items():
        print(f"\n{'='*50}")
        print(f"📍 ОБРАБОТКА ГОРОДА: {city_info['name']} ({city_slug})")
        print(f"{'='*50}")
        
        # Фильтруем данные по городу
        if 'city_slug' not in df_current.columns:
            print("❌ В данных нет колонки 'city_slug'")
            continue
            
        city_data = df_current[df_current['city_slug'] == city_slug]
        
        if len(city_data) == 0:
            print(f"⚠️ Нет данных для города {city_info['name']}")
            failed_reports += 1
            continue
        
        print(f"📊 Данных для анализа: {len(city_data)} записей")
        
        # Проверяем зарплатные данные
        monthly_mask = city_data['salary_period_name_normalized'].apply(is_monthly_salary)
        monthly_salary_count = city_data[monthly_mask & city_data['salary_to_net'].notna()].shape[0]
        print(f"💰 Зарплат 'за месяц': {monthly_salary_count}")
        
        # Фильтруем данные для сравнения
        prev_city_data = None
        if df_previous is not None and 'city_slug' in df_previous.columns:
            prev_city_data = df_previous[df_previous['city_slug'] == city_slug]
            if len(prev_city_data) > 0:
                print(f"📈 Данных для сравнения: {len(prev_city_data)} записей")
        
        try:
            # Анализируем метрики
            print(f"📈 Анализ метрик...")
            metrics = analyze_monthly_metrics(city_data, prev_city_data)
            
            # Создаем изображение
            print(f"🎨 Генерация изображения...")
            image_buf = create_monthly_report_image(city_info['name'], city_data, metrics)
            
            # Генерируем текст
            print(f"📝 Генерация текста...")
            text = generate_monthly_telegram_text(city_info['name'], metrics, month_start)
            
            # Отправляем в канал
            print(f"📤 Отправка в канал {city_info['channel']}...")
            await send_monthly_report(bot_token, city_info['channel'], image_buf, text)
            
            print(f"✅ Отчет для {city_info['name']} успешно отправлен!")
            successful_reports += 1
            
        except Exception as e:
            print(f"❌ Ошибка при обработке {city_info['name']}: {str(e)}")
            import traceback
            traceback.print_exc()
            failed_reports += 1
            continue
    
    # Итог
    print(f"\n{'='*50}")
    print("🎉 ИТОГ ВЫПОЛНЕНИЯ:")
    print(f"   Успешно отправлено: {successful_reports} отчетов")
    print(f"   С ошибками: {failed_reports} отчетов")
    print(f"   Всего городов: {len(CITIES)}")
    print(f"{'='*50}")
    
    if successful_reports == 0:
        print("⚠️ Внимание: ни один отчет не был отправлен!")

if __name__ == "__main__":
    asyncio.run(main_monthly_report())
