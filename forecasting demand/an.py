import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
warnings.filterwarnings('ignore')

# Настройка стилей
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# === ГЕНЕРАЦИЯ ДАННЫХ ===
np.random.seed(42)

# Генерация дат с января 2022 по апрель 2025
dates = pd.date_range(start='2022-01-01', end='2025-04-30', freq='M')

# Категории женской верхней одежды с их характеристиками
categories_info = {
    'Пальто': {'base_sales': 45, 'winter_boost': 2.0, 'summer_drop': 0.2},
    'Куртки': {'base_sales': 65, 'winter_boost': 1.8, 'summer_drop': 0.3},
    'Пуховики': {'base_sales': 55, 'winter_boost': 2.2, 'summer_drop': 0.1},
    'Плащи': {'base_sales': 35, 'winter_boost': 0.8, 'summer_drop': 0.6},
    'Жакеты': {'base_sales': 40, 'winter_boost': 1.1, 'summer_drop': 0.7}
}

n_skus = 150
categories = list(categories_info.keys())

# Генерация справочника артикулов
sku_catalog = pd.DataFrame({
    'sku': [f'SKU_{i:03d}' for i in range(1, n_skus+1)],
    'category': np.random.choice(categories, n_skus),
    'price': np.random.uniform(2000, 15000, n_skus)
})

# Генерация реалистичных данных продаж
def generate_seasonal_sales(date, category, base_sales):
    month = date.month
    
    # Определяем сезонные коэффициенты для каждой категории
    if month in [12, 1, 2]:  # Зима
        factor = categories_info[category]['winter_boost']
    elif month in [3, 4]:  # Ранняя весна
        factor = categories_info[category]['winter_boost'] * 0.7
    elif month in [5, 6]:  # Поздняя весна
        factor = 0.8
    elif month in [7, 8]:  # Лето
        factor = categories_info[category]['summer_drop']
    elif month in [9, 10]:  # Осень
        factor = 1.2
    else:  # Ноябрь - предзимье
        factor = categories_info[category]['winter_boost'] * 0.9
    
    # Добавляем трендовую составляющую (небольшой рост год к году)
    year_trend = 1 + (date.year - 2022) * 0.05
    
    # Генерируем продажи с учетом сезонности и тренда
    sales = np.random.poisson(base_sales * factor * year_trend)
    return max(0, sales)

# Создание данных по продажам
records = []
for date in dates:
    for _, row in sku_catalog.iterrows():
        base_sales = categories_info[row['category']]['base_sales']
        sales = generate_seasonal_sales(date, row['category'], base_sales)
        records.append({
            'date': date,
            'sku': row['sku'],
            'category': row['category'],
            'sales_units': sales,
            'sales_revenue': sales * row['price']
        })

sales_data = pd.DataFrame(records)

# === АНАЛИЗ СЕЗОННОСТИ ПО КАТЕГОРИЯМ ===
print("=== АНАЛИЗ СЕЗОННОСТИ ПО КАТЕГОРИЯМ ===")

category_seasonality = {}
category_decomposition = {}

fig, axes = plt.subplots(len(categories), 1, figsize=(15, 20))

for i, category in enumerate(categories):
    # Агрегируем данные по категории
    cat_data = sales_data[sales_data['category'] == category].groupby('date')['sales_units'].sum()
    
    # Разложение временного ряда
    decomposition = seasonal_decompose(cat_data, model='additive', period=12)
    category_decomposition[category] = decomposition
    
    # Извлекаем сезонные коэффициенты
    seasonal_pattern = decomposition.seasonal[:12].values
    category_seasonality[category] = seasonal_pattern
    
    # Визуализация
    axes[i].plot(cat_data.index, cat_data.values, label='Продажи', alpha=0.7)
    axes[i].plot(cat_data.index, decomposition.trend, label='Тренд', linewidth=2)
    axes[i].set_title(f'Динамика продаж: {category}')
    axes[i].legend()
    axes[i].grid(True)

plt.tight_layout()
plt.savefig('category_trends.png', dpi=300, bbox_inches='tight')
plt.show()

# === СОЗДАНИЕ СЕЗОННЫХ ПРОФИЛЕЙ ===
seasonal_df = pd.DataFrame(category_seasonality, 
                          index=['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн',
                                'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек'])

print("\n=== СЕЗОННЫЕ КОЭФФИЦИЕНТЫ ПО КАТЕГОРИЯМ ===")
print(seasonal_df.round(2))

# Тепловая карта сезонности
plt.figure(figsize=(12, 8))
sns.heatmap(seasonal_df.T, annot=True, cmap='RdYlBu_r', center=0, 
            fmt='.1f', cbar_kws={'label': 'Сезонный коэффициент'})
plt.title('Карта сезонности по категориям женской верхней одежды')
plt.ylabel('Категория')
plt.xlabel('Месяц')
plt.tight_layout()
plt.savefig('seasonality_heatmap.png', dpi=300, bbox_inches='tight')
plt.show()

# === ПРОГНОЗИРОВАНИЕ ПО КАТЕГОРИЯМ ===
print("\n=== ПРОГНОЗИРОВАНИЕ ПРОДАЖ ===")

forecast_horizon = 12
category_forecasts = {}
forecast_accuracy = {}

for category in categories:
    # Подготовка данных для категории
    cat_data = sales_data[sales_data['category'] == category].groupby('date')['sales_units'].sum()
    
    # Разделение на train/test для валидации
    train_size = len(cat_data) - 6
    train_data = cat_data[:train_size]
    test_data = cat_data[train_size:]
    
    # Построение модели
    try:
        model = ExponentialSmoothing(train_data, 
                                   seasonal='add', 
                                   seasonal_periods=12).fit()
        
        # Прогноз на тестовый период
        test_forecast = model.forecast(len(test_data))
        
        # Оценка точности
        mae = mean_absolute_error(test_data, test_forecast)
        rmse = np.sqrt(mean_squared_error(test_data, test_forecast))
        mape = np.mean(np.abs((test_data - test_forecast) / test_data)) * 100
        
        forecast_accuracy[category] = {'MAE': mae, 'RMSE': rmse, 'MAPE': mape}
        
        # Прогноз на будущее
        full_model = ExponentialSmoothing(cat_data, 
                                        seasonal='add', 
                                        seasonal_periods=12).fit()
        future_forecast = full_model.forecast(forecast_horizon)
        category_forecasts[category] = future_forecast
        
        print(f"{category}: MAE={mae:.2f}, RMSE={rmse:.2f}, MAPE={mape:.2f}%")
        
    except Exception as e:
        print(f"Ошибка для категории {category}: {e}")

# === АГРЕГИРОВАННЫЙ ПРОГНОЗ ===
# Создание общего прогноза
forecast_dates = pd.date_range(start='2025-05-31', periods=forecast_horizon, freq='M')
total_forecast = pd.Series(0, index=forecast_dates)

for category, forecast in category_forecasts.items():
    total_forecast += forecast

# === ВИЗУАЛИЗАЦИЯ ПРОГНОЗОВ ===
fig, axes = plt.subplots(2, 3, figsize=(18, 12))
axes = axes.flatten()

for i, category in enumerate(categories):
    if category in category_forecasts:
        # Исторические данные
        cat_data = sales_data[sales_data['category'] == category].groupby('date')['sales_units'].sum()
        
        # Прогноз
        forecast = category_forecasts[category]
        
        axes[i].plot(cat_data.index, cat_data.values, label='История', color='blue')
        axes[i].plot(forecast.index, forecast.values, label='Прогноз', 
                    color='red', linestyle='--', linewidth=2)
        axes[i].set_title(f'Прогноз: {category}')
        axes[i].legend()
        axes[i].grid(True)
        axes[i].tick_params(axis='x', rotation=45)

# Общий прогноз
total_historical = sales_data.groupby('date')['sales_units'].sum()
axes[5].plot(total_historical.index, total_historical.values, 
            label='История (общая)', color='blue')
axes[5].plot(total_forecast.index, total_forecast.values, 
            label='Прогноз (общий)', color='red', linestyle='--', linewidth=2)
axes[5].set_title('Общий прогноз продаж')
axes[5].legend()
axes[5].grid(True)
axes[5].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig('category_forecasts.png', dpi=300, bbox_inches='tight')
plt.show()

# === БИЗНЕС-АНАЛИЗ И РЕКОМЕНДАЦИИ ===
print("\n=== БИЗНЕС-АНАЛИЗ ===")

# Анализ пиковых месяцев по категориям
peak_months = {}
for category in categories:
    seasonal_pattern = category_seasonality[category]
    peak_month = np.argmax(seasonal_pattern) + 1
    peak_months[category] = peak_month
    month_names = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн',
                   'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
    print(f"{category}: пик продаж в {month_names[peak_month-1]}")

# Прогноз выручки
forecast_revenue = {}
for category in categories:
    if category in category_forecasts:
        avg_price = sku_catalog[sku_catalog['category'] == category]['price'].mean()
        forecast_units = category_forecasts[category].sum()
        forecast_revenue[category] = forecast_units * avg_price

print(f"\n=== ПРОГНОЗ ВЫРУЧКИ НА {forecast_horizon} МЕСЯЦЕВ ===")
total_revenue_forecast = 0
for category, revenue in forecast_revenue.items():
    print(f"{category}: {revenue:,.0f} руб.")
    total_revenue_forecast += revenue

print(f"ИТОГО: {total_revenue_forecast:,.0f} руб.")

# === РЕКОМЕНДАЦИИ ===
print("\n=== РЕКОМЕНДАЦИИ ДЛЯ БИЗНЕСА ===")
print("1. ЗАКУПКИ И СКЛАДСКИЕ ЗАПАСЫ:")
for category in categories:
    if category in peak_months:
        peak_month = peak_months[category]
        prep_month = peak_month - 2 if peak_month > 2 else peak_month + 10
        month_names = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн',
                       'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
        print(f"   {category}: увеличить закупки к {month_names[prep_month-1]}")

print("\n2. МАРКЕТИНГОВЫЕ КАМПАНИИ:")
high_season_categories = [cat for cat, month in peak_months.items() if month in [11, 12, 1, 2]]
low_season_categories = [cat for cat, month in peak_months.items() if month in [6, 7, 8]]

print(f"   Зимние категории ({', '.join(high_season_categories)}): усилить рекламу в октябре-ноябре")
print(f"   Летние категории ({', '.join(low_season_categories)}): летние акции и скидки")

print("\n3. ФИНАНСОВОЕ ПЛАНИРОВАНИЕ:")
print(f"   Ожидаемая выручка: {total_revenue_forecast:,.0f} руб.")
print(f"   Пиковый месяц общих продаж: {month_names[np.argmax([sum(category_seasonality[cat][i] for cat in categories) for i in range(12)])]}")

# Сохранение результатов
results_summary = pd.DataFrame({
    'Категория': categories,
    'Пиковый_месяц': [month_names[peak_months[cat]-1] for cat in categories],
    'Прогноз_продаж': [category_forecasts[cat].sum() if cat in category_forecasts else 0 for cat in categories],
    'Прогноз_выручки': [forecast_revenue.get(cat, 0) for cat in categories],
    'Точность_MAPE': [forecast_accuracy[cat]['MAPE'] if cat in forecast_accuracy else 0 for cat in categories]
})

print("\n=== СВОДНАЯ ТАБЛИЦА ===")
print(results_summary.round(2))

# Сохранение данных
sales_data.to_csv('sales_data_women_outerwear.csv', index=False)
results_summary.to_csv('forecast_summary.csv', index=False)

print("\n=== ПРОЕКТ ЗАВЕРШЁН ===")
print("Файлы сохранены:")
print("- sales_data_women_outerwear.csv")
print("- forecast_summary.csv")
print("- category_trends.png")
print("- seasonality_heatmap.png")
print("- category_forecasts.png")
