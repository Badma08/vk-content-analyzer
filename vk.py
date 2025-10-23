import requests
import csv
from datetime import datetime
import sqlite3
import pandas as pd

def get_vk_posts(domain, access_token):
    url = 'https://api.vk.com/method/wall.get'
    params = {
        'domain': domain,
        'count': 100,  # Можно получить до 100 постов за один запрос
        'access_token': access_token,
        'v': '5.131',
        'rev': 1
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if 'error' in data:
        print(f"Ошибка: {data['error']['error_msg']}")
        return None
    
    return data['response']['items']

# Используем Service Token (работает для публичных страниц)
domain = 'pravdashowtop' # Анализируемая страница
service_token = "7dba3dad7dba3dad7dba3dad1f7e81b81e77dba7dba3dad15568d124041258ff7365ea7"

posts = get_vk_posts('durov', service_token)

if posts:
    # Создаем CSV файл
    with open('posts_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['post_id', 'date', 'likes_count', 'hour', 'day_of_week'])
        
        for post in posts:
            post_id = post['id']
            date = datetime.fromtimestamp(post['date'])
            likes_count = post['likes']['count']
            hour = date.hour
            day_of_week = date.weekday()  # 0=Понедельник, 6=Воскресенье
            
            writer.writerow([post_id, date, likes_count, hour, day_of_week])
    
    print(f"Собрано {len(posts)} постов")

# Читаем CSV
df = pd.read_csv('posts_data.csv')

# Создаем базу данных в памяти
conn = sqlite3.connect(':memory:')
df.to_sql('posts', conn, index=False)


# ЗАПРОС 1: Анализ влияния времени суток (часа публикации)
print("\nВЛИЯНИЕ ВРЕМЕНИ СУТОК (ЧАСА ПУБЛИКАЦИИ)")
print("─" * 50)
result1 = pd.read_sql_query("""
--- Группируем по часам и смотрим на среднее количество лайков
--- HAVING-условие отфильтровывает часы с малым количеством постов 
--- (меньше 50% от среднего количества постов в час) для статистической значимости
SELECT
    hour,
    AVG(likes_count) AS avg_likes,
    COUNT(*) AS posts_count
FROM
    posts
GROUP BY
    hour
having posts_count >= (SELECT AVG(posts_count)
                      from (SELECT COUNT(*) as posts_count
                            from posts
                            group by hour)
                        ) * 0.5
ORDER BY
    avg_likes DESC;
""", conn)

print(result1)

print("\nВЛИЯНИЕ ДНЯ НЕДЕЛИ")
print("─" * 50)
result2 = pd.read_sql_query("""
-- Группируем по дню недели и смотрим на среднее количество лайков
--- HAVING-условие отфильтровывает дни недели с малым количеством постов
--- (меньше 50% от среднего количества постов в час) для статистической значимости
SELECT
    day_of_week,
    AVG(likes_count) AS avg_likes,
    COUNT(*) AS posts_count
FROM
    posts
GROUP BY
    day_of_week
having posts_count >= (SELECT AVG(posts_count)
                      from (SELECT COUNT(*) as posts_count
                            from posts
                            group by day_of_week)
                        ) * 0.5
ORDER BY
    avg_likes DESC;
""", conn)
print(result2)

print("\nВЛИЯНИЕ ПРОМЕЖУТКА МЕЖДУ ПОСТАМИ")
print("─" * 50)

result3 = pd.read_sql_query("""
-- Группируем по промежутку времени между постами и смотрим на среднее количество лайков
WITH posts_with_prev_time AS (
    SELECT 
        *,
        LAG(date) OVER (ORDER BY date) AS previous_post_date,
        JULIANDAY(date) - JULIANDAY(LAG(date) OVER (ORDER BY date)) AS days_since_previous_post,
        (JULIANDAY(date) - JULIANDAY(LAG(date) OVER (ORDER BY date))) * 24 AS hours_since_previous_post
    FROM posts
)
SELECT  
    CASE  
        WHEN hours_since_previous_post < 1 THEN 'Менее 1 часа'  
        WHEN hours_since_previous_post BETWEEN 1 AND 3 THEN '1-3 часа'  
        WHEN hours_since_previous_post BETWEEN 3 AND 6 THEN '3-6 часов'  
        WHEN hours_since_previous_post BETWEEN 6 AND 12 THEN '6-12 часов'  
        WHEN hours_since_previous_post BETWEEN 12 AND 24 THEN '12-24 часа'  
        ELSE 'Более 1 дня'  
    END AS time_gap,
    AVG(likes_count) AS avg_likes,  
    COUNT(*) AS posts_count  
FROM  
    posts_with_prev_time  
WHERE  
    hours_since_previous_post IS NOT NULL  
GROUP BY  
    time_gap
HAVING posts_count >= (select count(post_id)
                        from posts) / count(time_gap) * 0.5
ORDER BY  
    avg_likes DESC;
""", conn)
print(result3)

print("\nИТОГОВОЕ СРАВНЕНИЕ ФАКТОРОВ")
result_final = pd.read_sql_query("""
WITH
-- Анализ по часам: группируем посты по часам публикации и вычисляем средние лайки
-- ФИЛЬТРАЦИЯ: оставляем только те часы, где количество постов >= 50% от среднего количества постов в час
-- Это исключает редкоиспользуемые часы с недостаточной статистической значимостью
hour_analysis AS (
    SELECT AVG(likes_count) AS avg_likes
    FROM posts
    GROUP BY hour
    HAVING COUNT(*) >= (SELECT AVG(posts_count) * 0.5
                       FROM (SELECT COUNT(*) as posts_count
                             FROM posts
                             GROUP BY hour))
),

-- Анализ по дням недели: группируем посты по дням недели и вычисляем средние лайки
-- ФИЛЬТРАЦИЯ: оставляем только те дни, где количество постов >= 50% от среднего количества постов в день
-- Это обеспечивает сравнение только статистически значимых дней
day_analysis AS (
    SELECT AVG(likes_count) AS avg_likes
    FROM posts
    GROUP BY day_of_week
    HAVING COUNT(*) >= (SELECT AVG(posts_count) * 0.5
                       FROM (SELECT COUNT(*) as posts_count
                             FROM posts
                             GROUP BY day_of_week))
),

-- Анализ по временным промежуткам: изучаем влияние интервалов между постами на engagement
-- Используем оконную функцию LAG для определения времени предыдущего поста
-- Переводим разницу во времени в часы и группируем по категориям
gap_analysis AS (
    WITH posts_with_prev_time AS (
        SELECT (JULIANDAY(date) - JULIANDAY(LAG(date) OVER (ORDER BY date))) * 24 AS hours_gap, likes_count
        FROM posts
    )
    SELECT
        -- Категоризация временных промежутков для анализа паттернов публикации
        CASE
            WHEN hours_gap < 1 THEN 'Менее 1 часа'
            WHEN hours_gap BETWEEN 1 AND 3 THEN '1-3 часа'
            WHEN hours_gap BETWEEN 3 AND 6 THEN '3-6 часов'
            WHEN hours_gap BETWEEN 6 AND 12 THEN '6-12 часов'
            WHEN hours_gap BETWEEN 12 AND 24 THEN '12-24 часа'
            ELSE 'Более 1 дня'
        END AS gap,
        AVG(likes_count) AS avg_likes
    FROM posts_with_prev_time
    WHERE hours_gap IS NOT NULL  -- Исключаем первую запись (нет предыдущего поста)
    GROUP BY gap
    -- ФИЛЬТРАЦИЯ: оставляем промежутки с достаточным количеством постов
    -- Рассчитываем минимальный порог как 50% от среднего количества постов на категорию
    -- (общее количество постов / 6 возможных категорий * 0.5)
    HAVING COUNT(*) >= (SELECT COUNT(*) FROM posts) / 6 * 0.5
),

-- Сравнение силы влияния факторов: вычисляем разброс средних значений лайков
-- для каждого фактора (разница между лучшим и худшим показателем)
comparison AS (
    SELECT 'Время суток' AS factor,
           -- Сила влияния = разница между максимальным и минимальным средним количеством лайков по часам
           (SELECT MAX(avg_likes) - MIN(avg_likes) FROM hour_analysis) AS influence
    UNION ALL
    SELECT 'День недели',
           -- Сила влияния = разница между максимальным и минимальным средним количеством лайков по дням
           (SELECT MAX(avg_likes) - MIN(avg_likes) FROM day_analysis)
    UNION ALL
    SELECT 'Промежуток между постами',
           -- Сила влияния = разница между максимальным и минимальным средним количеством лайков по промежуткам
           (SELECT MAX(avg_likes) - MIN(avg_likes) FROM gap_analysis)
)

-- Финальный результат: выводим факторы в порядке убывания силы влияния
-- Чем больше значение "Сила влияния", тем сильнее данный фактор влияет на количество лайков
SELECT factor AS "Фактор",
       ROUND(influence, 2) AS "Сила влияния (разница в лайках)"
FROM comparison
ORDER BY influence DESC;  -- Сортируем по убыванию силы влияния
""", conn)

print(result_final)

# Определяем, какой фактор самый сильный
strongest_factor = result_final.iloc[0]["Фактор"]
print(f"\nНаибольшее влияние на количество лайков оказывает: {strongest_factor}")
