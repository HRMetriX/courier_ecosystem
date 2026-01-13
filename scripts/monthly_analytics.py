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

def get_month_range(report_date: datetime) -> Tuple[datetime, datetime]:
    """Получить начало и конец месяца для отчета"""
    first_day = report_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if report_date.month == 12:
        last_day = report_date.replace(year=report_date.year + 1, month=1, day=1)
    else:
        last_day = report_date.replace(month=report_date.month + 1, day=1)
    last_day = last_day - timedelta(seconds=1)
    return first_day, last_day

def get_previous_month_range(report_date: datetime) -> Tuple[datetime, datetime]:
    """Получить начало и конец предыдущего месяца"""
    if report_date.month == 1:
        prev_month = report_date.replace(year=report_date.year - 1, month=12, day=1)
    else:
        prev_month = report_date.replace(month=report_date.month - 1, day=1)
    return get_month_range(prev_month)

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
        print(f"Загружено страниц: {page}, всего строк: {len(all_data)}")

    df = pd.DataFrame(all_data)
    print(f"\n✅ Итого загружено {len(df)} строк за месяц")
    
    # Если данных нет, возвращаем пустой DataFrame
    if len(df) == 0:
        print("⚠️ Нет данных за указанный период")
        return pd.DataFrame()
    
    # Преобразуем даты
    if 'published_at' in df.columns:
        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
        df['published_at_moscow'] = df['published_at'].dt.tz_convert(MOSCOW_TZ)
        df['published_date'] = df['published_at_moscow'].dt.date
        df['published_day'] = df['published_at_moscow'].dt.day
        df['published_week'] = df['published_at_moscow'].dt.isocalendar().week
        df['published_weekday'] = df['published_at_moscow'].dt.day_name()
    
    return df

def analyze_monthly_metrics(city_data: pd.DataFrame, prev_month_data: pd.DataFrame = None) -> Dict:
    """Анализ основных метрик за месяц"""
    
    # Только месячные зарплаты
    monthly_salary_data = city_data[
        city_data['salary_period_name'].str.contains('месяц', case=False, na=False) & 
        city_data['salary_to_net'].notna()
    ]
    
    metrics = {}
    
    # 1. Базовые метрики
    metrics['total_vacancies'] = len(city_data)
    metrics['with_monthly_salary'] = len(monthly_salary_data)
    metrics['salary_percentage'] = (metrics['with_monthly_salary'] / metrics['total_vacancies'] * 100) if metrics['total_vacancies'] > 0 else 0
    
    # 2. Зарплатные метрики
    if len(monthly_salary_data) > 0:
        metrics['avg_salary'] = monthly_salary_data['salary_to_net'].mean()
        metrics['median_salary'] = monthly_salary_data['salary_to_net'].median()
        metrics['salary_std'] = monthly_salary_data['salary_to_net'].std()
        
        # Квартили
        metrics['q25'] = monthly_salary_data['salary_to_net'].quantile(0.25)
        metrics['q75'] = monthly_salary_data['salary_to_net'].quantile(0.75)
        metrics['q90'] = monthly_salary_data['salary_to_net'].quantile(0.90)
        metrics['q10'] = monthly_salary_data['salary_to_net'].quantile(0.10)
        
        # Зарплатные вилки
        salary_with_range = monthly_salary_data[monthly_salary_data['salary_from_net'].notna()]
        if len(salary_with_range) > 0:
            metrics['avg_salary_range'] = (salary_with_range['salary_to_net'] - salary_with_range['salary_from_net']).mean()
            metrics['vacancies_with_range'] = len(salary_with_range)
        else:
            metrics['avg_salary_range'] = 0
            metrics['vacancies_with_range'] = 0
        
        # Анализ динамики зарплат по дням для EMA
        daily_avg_salary = monthly_salary_data.groupby('published_day')['salary_to_net'].mean()
        if len(daily_avg_salary) >= 7:
            ema_series = calculate_ema(daily_avg_salary.sort_index(), span=7)
            trend_analysis = analyze_trend_from_ema(ema_series)
            metrics['trend_analysis'] = trend_analysis
    
    # 3. Анализ графиков работы
    if 'schedule_name' in city_data.columns:
        schedule_counts = city_data['schedule_name'].value_counts()
        metrics['top_schedules'] = schedule_counts.head(3).to_dict()
        metrics['total_schedules'] = len(schedule_counts)
    
    # 4. Анализ дней недели
    if 'published_weekday' in city_data.columns:
        weekday_counts = city_data['published_weekday'].value_counts()
        metrics['top_weekday'] = weekday_counts.index[0] if len(weekday_counts) > 0 else None
        metrics['weekday_counts'] = weekday_counts.to_dict()
    
    # 5. ТОП работодателей
    if 'employer' in city_data.columns:
        employer_counts = city_data['employer'].value_counts().head(5)
        metrics['top_employers_count'] = employer_counts.to_dict()
        
        # ТОП работодателей по зарплате (только те, у кого > 5 вакансий)
        if len(monthly_salary_data) > 0:
            employer_avg_salary = monthly_salary_data.groupby('employer').agg({
                'salary_to_net': ['mean', 'count']
            }).round(0)
            employer_avg_salary.columns = ['avg_salary', 'vacancy_count']
            employer_avg_salary = employer_avg_salary[employer_avg_salary['vacancy_count'] >= 3]
            if len(employer_avg_salary) > 0:
                metrics['top_employers_salary'] = employer_avg_salary.nlargest(5, 'avg_salary')['avg_salary'].to_dict()
    
    # 6. Сравнение с предыдущим месяцем
    if prev_month_data is not None and len(prev_month_data) > 0 and 'city_slug' in prev_month_data.columns:
        prev_month_metrics = analyze_monthly_metrics(prev_month_data)
        
        metrics['prev_month_total'] = prev_month_metrics['total_vacancies']
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
    monthly_salary_data = city_data[
        (city_data['salary_period_name'] == 'За месяц') & 
        (city_data['salary_to_net'].notna())
    ]
    
    if len(monthly_salary_data) > 0:
        salaries = monthly_salary_data['salary_to_net']
        ax1.hist(salaries, bins=20, color='#3498db', edgecolor='white', alpha=0.7)
        ax1.axvline(metrics.get('avg_salary', 0), color='red', linestyle='--', 
                   linewidth=2, label=f'Средняя: {metrics.get("avg_salary", 0):,.0f} ₽')
        ax1.axvline(metrics.get('median_salary', 0), color='green', linestyle='--',
                   linewidth=2, label=f'Медиана: {metrics.get("median_salary", 0):,.0f} ₽')
        ax1.set_title('РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ ЗА МЕСЯЦ', fontsize=11, fontweight='bold')
        ax1.set_xlabel('Зарплата, ₽', fontsize=9)
        ax1.set_ylabel('Количество вакансий', fontsize=9)
        ax1.legend(fontsize=8)
        ax1.grid(True, alpha=0.3)
        
        # Форматирование осей
        ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    else:
        ax1.text(0.5, 0.5, 'Нет данных\nо зарплатах', ha='center', va='center', 
                fontsize=12, color='gray')
        ax1.set_title('РАСПРЕДЕЛЕНИЕ ЗАРПЛАТ ЗА МЕСЯЦ', fontsize=11, fontweight='bold')
    
    # 2. АКТИВНОСТЬ ПО НЕДЕЛЯМ (верхний правый)
    ax2 = plt.subplot(3, 2, 2)
    if 'published_week' in city_data.columns:
        weekly_counts = city_data.groupby('published_week').size()
        weeks = [f'Неделя {w}' for w in weekly_counts.index]
        bars = ax2.bar(weeks, weekly_counts.values, color='#9b59b6', alpha=0.7)
        ax2.set_title('АКТИВНОСТЬ ПО НЕДЕЛЯМ', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Количество вакансий', fontsize=9)
        ax2.tick_params(axis='x', rotation=45)
        
        # Добавляем значения на столбцы
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{int(height)}', ha='center', va='bottom', fontsize=8)
    else:
        ax2.text(0.5, 0.5, 'Нет данных\nпо неделям', ha='center', va='center',
                fontsize=12, color='gray')
        ax2.set_title('АКТИВНОСТЬ ПО НЕДЕЛЯМ', fontsize=11, fontweight='bold')
    
    # 3. ГРАФИКИ РАБОТЫ (средний левый)
    ax3 = plt.subplot(3, 2, 3)
    if 'schedule_name' in city_data.columns and not city_data['schedule_name'].isna().all():
        schedule_counts = city_data['schedule_name'].value_counts().head(5)
        colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6']
        wedges, texts, autotexts = ax3.pie(schedule_counts.values, labels=None,
                                          autopct='%1.1f%%', startangle=90,
                                          colors=colors[:len(schedule_counts)])
        ax3.set_title('РАСПРЕДЕЛЕНИЕ ПО ГРАФИКАМ РАБОТЫ', fontsize=11, fontweight='bold')
        
        # Легенда снаружи
        legend_labels = [f'{label} ({count})' for label, count in zip(schedule_counts.index, schedule_counts.values)]
        ax3.legend(wedges, legend_labels, title="Графики", loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1), fontsize=8)
    else:
        ax3.text(0.5, 0.5, 'Нет данных\nо графиках', ha='center', va='center',
                fontsize=12, color='gray')
        ax3.set_title('РАСПРЕДЕЛЕНИЕ ПО ГРАФИКАМ РАБОТЫ', fontsize=11, fontweight='bold')
    
    # 4. ДНИ НЕДЕЛИ (средний правый)
    ax4 = plt.subplot(3, 2, 4)
    if 'published_weekday' in city_data.columns:
        # Порядок дней недели
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        days_rus = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
        
        weekday_counts = city_data['published_weekday'].value_counts()
        # Приводим к правильному порядку
        ordered_counts = [weekday_counts.get(day, 0) for day in days_order]
        
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
                fontsize=12, color='gray')
        ax4.set_title('АКТИВНОСТЬ ПО ДНЯМ НЕДЕЛИ', fontsize=11, fontweight='bold')
    
    # 5. ТОП РАБОТОДАТЕЛИ ПО КОЛИЧЕСТВУ (нижний левый)
    ax5 = plt.subplot(3, 2, 5)
    if 'employer' in city_data.columns:
        top_employers = city_data['employer'].value_counts().head(5)
        if len(top_employers) > 0:
            employers_short = [e[:15] + '...' if len(e) > 15 else e for e in top_employers.index]
            y_pos = np.arange(len(employers_short))
            bars = ax5.barh(y_pos, top_employers.values, color='#3498db', alpha=0.7)
            ax5.set_yticks(y_pos)
            ax5.set_yticklabels(employers_short, fontsize=8)
            ax5.invert_yaxis()
            ax5.set_title('ТОП-5 РАБОТОДАТЕЛЕЙ (по количеству)', fontsize=11, fontweight='bold')
            ax5.set_xlabel('Количество вакансий', fontsize=9)
            
            # Добавляем значения
            for i, v in enumerate(top_employers.values):
                ax5.text(v + 0.5, i, str(v), va='center', fontsize=8)
        else:
            ax5.text(0.5, 0.5, 'Нет данных\nо работодателях', ha='center', va='center',
                    fontsize=12, color='gray')
            ax5.set_title('ТОП-5 РАБОТОДАТЕЛЕЙ', fontsize=11, fontweight='bold')
    else:
        ax5.text(0.5, 0.5, 'Нет данных\nо работодателях', ha='center', va='center',
                fontsize=12, color='gray')
        ax5.set_title('ТОП-5 РАБОТОДАТЕЛЕЙ', fontsize=11, fontweight='bold')
    
    # 6. ДИНАМИКА ЗАРПЛАТ С EMA (нижний правый) - ОБНОВЛЕННЫЙ ГРАФИК!
    ax6 = plt.subplot(3, 2, 6)
    if 'published_day' in city_data.columns and len(monthly_salary_data) > 0:
        daily_avg_salary = monthly_salary_data.groupby('published_day')['salary_to_net'].mean()
        daily_median_salary = monthly_salary_data.groupby('published_day')['salary_to_net'].median()
        
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
            # Рисуем основные линии (средняя и медиана)
            avg_line, = ax6.plot(valid_days, valid_avg, 'o-', color='#3498db', 
                               label='Средняя за день', linewidth=2, markersize=4, alpha=0.7)
            median_line, = ax6.plot(valid_days, valid_median, 's--', color='#2ecc71', 
                                  label='Медиана за день', linewidth=1.5, markersize=3, alpha=0.7)
            
            # ДОБАВЛЯЕМ EMA ДЛЯ СРЕДНЕЙ ЗАРПЛАТЫ
            if len(valid_avg) >= 7:
                # Вычисляем EMA с периодом 7 дней
                ema_series = calculate_ema(pd.Series(valid_avg, index=valid_days), span=7)
                ema_line, = ax6.plot(valid_days, ema_series.values, color='#e74c3c', 
                                   linewidth=3, label='Тренд (EMA7)', alpha=0.8, zorder=5)
                
                # Добавляем заливку между средней и EMA
                ax6.fill_between(valid_days, valid_avg, ema_series.values,
                               alpha=0.15, color='#e74c3c', label='Отклонение от тренда')
                
                # Анализируем тренд
                trend_info = analyze_trend_from_ema(ema_series)
                
                # Добавляем аннотацию тренда
                if trend_info:
                    trend_text = f"{trend_info['emoji']} {trend_info['direction'].upper()}\n{trend_info['strength']} {trend_info['pct']:+.1f}%"
                    
                    # Размещаем аннотацию в правом верхнем углу графика
                    ax6.annotate(trend_text, xy=(0.98, 0.95), xycoords='axes fraction',
                               fontsize=9, color='#e74c3c', fontweight='bold',
                               ha='right', va='top',
                               bbox=dict(boxstyle="round,pad=0.3", 
                                        facecolor='white', 
                                        edgecolor='#e74c3c',
                                        alpha=0.9))
            
            # Настройки графика
            ax6.set_title('ДИНАМИКА ЗАРПЛАТ С АНАЛИЗОМ ТРЕНДА', fontsize=11, fontweight='bold')
            ax6.set_xlabel('День месяца', fontsize=9)
            ax6.set_ylabel('Зарплата, ₽', fontsize=9)
            ax6.legend(fontsize=8, loc='lower center', bbox_to_anchor=(0.5, -0.35), 
                      ncol=2, framealpha=0.9)
            ax6.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
            ax6.set_xticks(range(1, 32, 5))
            
            # Форматирование оси Y
            ax6.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
            
            # Автоматическое масштабирование
            all_values = valid_avg + valid_median
            if len(all_values) > 0:
                y_min, y_max = min(all_values), max(all_values)
                y_range = y_max - y_min
                ax6.set_ylim(y_min - y_range*0.1, y_max + y_range*0.1)
        else:
            ax6.text(0.5, 0.5, 'Недостаточно данных\nдля анализа тренда', 
                    ha='center', va='center', fontsize=12, color='gray')
            ax6.set_title('ДИНАМИКА ЗАРПЛАТ С АНАЛИЗОМ ТРЕНДА', fontsize=11, fontweight='bold')
    else:
        ax6.text(0.5, 0.5, 'Нет данных\nдля анализа тренда', 
                ha='center', va='center', fontsize=12, color='gray')
        ax6.set_title('ДИНАМИКА ЗАРПЛАТ С АНАЛИЗОМ ТРЕНДА', fontsize=11, fontweight='bold')
    
    # Общий заголовок
    month_name = datetime.now().strftime('%B %Y').upper()
    fig.suptitle(f'МЕСЯЧНЫЙ ОТЧЕТ: {city_name.upper()} - {month_name}', 
                fontsize=14, fontweight='bold', y=0.98)
    
    # Оптимизируем layout
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
    """Генерация текста месячного отчета для Telegram с учетом тренда EMA"""
    
    # Название месяца на русском
    month_names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }
    month_name = month_names[month_start.month]
    year = month_start.year
    
    # Форматирование чисел
    def format_num(num):
        return f"{num:,.0f}".replace(",", " ")
    
    # Форматирование денег
    def format_money(num):
        return f"{format_num(num)} ₽"
    
    # Форматирование процентов
    def format_pct(num):
        return f"{num:+.1f}%" if num != 0 else "0%"
    
    # Строим сообщение
    message = f"📊 *МЕСЯЧНЫЙ ОТЧЕТ: {city_name.upper()} - {month_name} {year}*\n\n"
    
    # 1. ОСНОВНЫЕ ПОКАЗАТЕЛИ
    message += "📈 *ОСНОВНЫЕ ПОКАЗАТЕЛИ:*\n"
    message += f"• Всего вакансий: *{format_num(metrics.get('total_vacancies', 0))}*\n"
    message += f"• С зарплатой 'за месяц': *{format_num(metrics.get('with_monthly_salary', 0))}* "
    message += f"({metrics.get('salary_percentage', 0):.1f}%)\n"
    
    if 'avg_salary' in metrics:
        message += f"• Средняя зарплата: *{format_money(metrics['avg_salary'])}*\n"
        message += f"• Медианная зарплата: *{format_money(metrics['median_salary'])}*\n"
        
        # Квартили
        message += f"• 25% получают до: *{format_money(metrics.get('q25', 0))}*\n"
        message += f"• 75% получают до: *{format_money(metrics.get('q75', 0))}*\n"
        message += f"• ТОП-10%: от *{format_money(metrics.get('q90', 0))}*\n"
    
    # 2. ТРЕНД ПО EMA (НОВОЕ!)
    if 'trend_analysis' in metrics:
        trend = metrics['trend_analysis']
        message += f"\n{trend['emoji']} *ТРЕНД ЗАРПЛАТ (EMA7):*\n"
        message += f"• Направление: *{trend['direction']}*\n"
        message += f"• Изменение: *{format_pct(trend['pct'])}*\n"
        message += f"• С *{format_money(trend['start'])}* до *{format_money(trend['end'])}*\n"
        message += f"• Сила тренда: *{trend['strength']}*\n"
    
    # 3. СРАВНЕНИЕ С ПРОШЛЫМ МЕСЯЦЕМ
    if 'total_growth' in metrics:
        growth_emoji = "📈" if metrics['total_growth'] > 0 else "📉" if metrics['total_growth'] < 0 else "➡️"
        message += f"\n{growth_emoji} *СРАВНЕНИЕ С ПРОШЛЫМ МЕСЯЦЕМ:*\n"
        message += f"• Вакансий: *{format_pct(metrics['total_growth_pct'])}* "
        message += f"({format_num(metrics['total_growth'])})\n"
        
        if 'salary_growth' in metrics:
            salary_emoji = "💰📈" if metrics['salary_growth'] > 0 else "💰📉" if metrics['salary_growth'] < 0 else "💰➡️"
            message += f"• {salary_emoji} Зарплата: *{format_pct(metrics['salary_growth_pct'])}* "
            message += f"({format_money(metrics['salary_growth'])})\n"
    
    # 4. АНАЛИЗ ГРАФИКОВ РАБОТЫ
    message += "\n⏰ *ПОПУЛЯРНЫЕ ГРАФИКИ:*\n"
    if 'top_schedules' in metrics and metrics['top_schedules']:
        for schedule, count in list(metrics['top_schedules'].items())[:3]:
            pct = (count / metrics['total_vacancies'] * 100) if metrics['total_vacancies'] > 0 else 0
            message += f"• {schedule}: *{count}* ({pct:.1f}%)\n"
    else:
        message += "• Нет данных о графиках\n"
    
    # 5. ДНИ НЕДЕЛИ
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
    
    # 6. ТОП РАБОТОДАТЕЛИ
    message += "\n🏢 *ТОП РАБОТОДАТЕЛИ:*\n"
    if 'top_employers_count' in metrics and metrics['top_employers_count']:
        message += "*По количеству вакансий:*\n"
        for i, (employer, count) in enumerate(metrics['top_employers_count'].items(), 1):
            employer_short = employer[:20] + '...' if len(employer) > 20 else employer
            message += f"{i}. {employer_short}: *{count}*\n"
    
    if 'top_employers_salary' in metrics and metrics['top_employers_salary']:
        message += "\n*По средней зарплате:*\n"
        for i, (employer, salary) in enumerate(metrics['top_employers_salary'].items(), 1):
            employer_short = employer[:20] + '...' if len(employer) > 20 else employer
            message += f"{i}. {employer_short}: *{format_money(salary)}*\n"
    
    # 7. ЗАРПЛАТНЫЕ ВИЛКИ
    if 'vacancies_with_range' in metrics and metrics['vacancies_with_range'] > 0:
        message += f"\n💰 *ЗАРПЛАТНЫЕ ВИЛКИ:*\n"
        message += f"• Вакансий с вилкой: *{format_num(metrics['vacancies_with_range'])}*\n"
        message += f"• Средняя вилка: *{format_money(metrics.get('avg_salary_range', 0))}*\n"
    
    # 8. СТАТИСТИЧЕСКИЕ ИНСАЙТЫ
    message += "\n🔍 *СТАТИСТИЧЕСКИЕ ИНСАЙТЫ:*\n"
    
    if 'salary_std' in metrics and metrics.get('avg_salary', 0) > 0:
        cv = (metrics['salary_std'] / metrics['avg_salary']) * 100
        volatility = "высокая" if cv > 30 else "средняя" if cv > 15 else "низкая"
        message += f"• Волатильность зарплат: *{volatility}* ({cv:.1f}%)\n"
    
    if 'q90' in metrics and 'q10' in metrics and metrics['q10'] > 0:
        ratio = metrics['q90'] / metrics['q10']
        inequality = "высокое" if ratio > 2.5 else "среднее" if ratio > 1.8 else "низкое"
        message += f"• Неравенство зарплат: *{inequality}* (x{ratio:.1f})\n"
    
    # Время обновления
    moscow_now = datetime.now(MOSCOW_TZ)
    update_time = moscow_now.strftime('%d.%m.%Y %H:%M')
    message += f"\n⏰ *Отчет сгенерирован:* {update_time} МСК\n"
    
    return message

async def send_monthly_report(bot_token: str, channel: str, image_buf: BytesIO, text: str):
    """Отправка месячного отчета в Telegram канал"""
    bot = Bot(token=bot_token)
    
    # Отправляем изображение с подписью
    image_buf.seek(0)
    await bot.send_photo(chat_id=channel, photo=image_buf, caption=text, parse_mode='Markdown')
    
    print(f"✅ Месячный отчет отправлен в канал {channel}")

async def main_monthly_report():
    """Основная функция для генерации месячного отчета с EMA"""
    print("🚀 Запуск генерации МЕСЯЧНОГО отчета с анализом трендов...")
    
    # Определяем период отчета (предыдущий месяц)
    report_date = datetime.now(MOSCOW_TZ)
    
    # Для тестирования можно задать конкретную дату
    # report_date = datetime(2024, 1, 1, tzinfo=MOSCOW_TZ)
    
    # Получаем данные за отчетный месяц
    month_start, month_end = get_month_range(report_date)
    print(f"📅 Отчетный период: {month_start.strftime('%d.%m.%Y')} - {month_end.strftime('%d.%m.%Y')}")
    
    # Получаем данные за предыдущий месяц для сравнения
    prev_month_start, prev_month_end = get_previous_month_range(report_date)
    
    # Загружаем данные
    print("📦 Загружаем данные из Supabase...")
    
    # Текущий месяц
    df_current = load_monthly_data_from_supabase(month_start, month_end)
    
    # Предыдущий месяц (для сравнения)
    try:
        df_previous = load_monthly_data_from_supabase(prev_month_start, prev_month_end)
        print(f"📊 Данные за предыдущий месяц: {len(df_previous)} строк")
        
        # Если данных нет или DataFrame пустой
        if df_previous is None or len(df_previous) == 0:
            print("ℹ️ Нет данных за предыдущий месяц для сравнения")
            df_previous = None
        elif 'city_slug' not in df_previous.columns:
            print("⚠️ В данных за предыдущий месяц нет колонки 'city_slug'")
            print(f"   Доступные колонки: {list(df_previous.columns)}")
            df_previous = None
            
    except Exception as e:
        print(f"⚠️ Не удалось загрузить данные за предыдущий месяц: {e}")
        df_previous = None
    
    # Проверяем наличие данных
    if len(df_current) == 0:
        print("❌ Нет данных за отчетный период")
        return
    
    # Получаем токен бота
    bot_token = os.environ.get("TG_BOT_TOKEN")
    if not bot_token:
        raise ValueError("Не найдена переменная окружения TG_BOT_TOKEN")
    
    # Проходим по каждому городу
    for city_slug, city_info in CITIES.items():
        print(f"\n📍 Обработка города: {city_info['name']} ({city_slug})")
        
        # Фильтруем данные по городу
        city_data = df_current[df_current['city_slug'] == city_slug]
        
        if len(city_data) == 0:
            print(f"⚠️ Нет данных для города {city_info['name']} за отчетный месяц")
            continue
        
        # Фильтруем предыдущие данные для этого города
        prev_city_data = None
        if df_previous is not None and len(df_previous) > 0 and 'city_slug' in df_previous.columns:
            prev_city_data = df_previous[df_previous['city_slug'] == city_slug]
            if len(prev_city_data) > 0:
                print(f"📊 Данные за предыдущий месяц для {city_info['name']}: {len(prev_city_data)} строк")
            else:
                print(f"ℹ️ Нет данных за предыдущий месяц для {city_info['name']}")
                prev_city_data = None
        else:
            print(f"ℹ️ Нет данных за предыдущий месяц для сравнения с {city_info['name']}")
        
        try:
            # Анализируем метрики
            print(f"📊 Анализируем метрики для {city_info['name']}...")
            metrics = analyze_monthly_metrics(city_data, prev_city_data)
            
            # Создаем изображение
            print(f"🎨 Генерируем изображение отчета для {city_info['name']}...")
            image_buf = create_monthly_report_image(city_info['name'], city_data, metrics)
            
            # Генерируем текст
            print(f"📝 Генерируем текст отчета для {city_info['name']}...")
            text = generate_monthly_telegram_text(city_info['name'], metrics, month_start)
            
            # Отправляем в канал
            print(f"📤 Отправляем отчет в канал {city_info['channel']}...")
            await send_monthly_report(bot_token, city_info['channel'], image_buf, text)
            
            print(f"✅ Месячный отчет для {city_info['name']} успешно отправлен!")
            
            # Выводим информацию о тренде в консоль
            if 'trend_analysis' in metrics:
                trend = metrics['trend_analysis']
                print(f"   📊 Тренд EMA7: {trend['emoji']} {trend['direction']} ({trend['pct']:+.1f}%)")
            
        except Exception as e:
            print(f"❌ Ошибка при обработке {city_info['name']}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n🎉 Все месячные отчеты успешно отправлены!")

if __name__ == "__main__":
    # Для запуска вручную
    asyncio.run(main_monthly_report())
