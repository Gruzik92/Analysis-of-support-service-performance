-- 1. Початковий запит
SELECT
    id_request,
    moderator,
    team,
    request_time,
    start_time,
    finish_time,
    EXTRACT(EPOCH FROM (start_time - request_time)) / 60 AS wait_time_min,
    EXTRACT(EPOCH FROM (finish_time - start_time)) / 60 AS handling_time_min,
    CASE
        WHEN EXTRACT(EPOCH FROM (start_time - request_time)) / 60 <= 15 THEN 'до 15 хв (відповідає очікуванням)'
        WHEN EXTRACT(EPOCH FROM (start_time - request_time)) / 60 <= 45 THEN '15–45 хв (ще допустимо)'
        ELSE 'більше 45 хв (погано)'
    END AS sla_category
FROM events;

-- 2. Середній час до відповіді (у хвилинах)
SELECT 
    ROUND(AVG(EXTRACT(EPOCH FROM start_time - request_time) / 60), 1) AS avg_response_time_minutes
FROM events;

-- 3. Середній час до відповіді (у хвилинах) по командах
SELECT 
    team,
    ROUND(AVG(EXTRACT(EPOCH FROM start_time - request_time) / 60), 1) AS avg_response_time_minutes
FROM events
WHERE start_time IS NOT NULL 
  AND request_time IS NOT NULL 
  AND start_time >= request_time
GROUP BY team
ORDER BY avg_response_time_minutes;

-- 4. SLA-категорії запитів
SELECT
    sla_category,
    COUNT(*) AS count_requests,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percent
FROM (
    SELECT
        CASE
            WHEN EXTRACT(EPOCH FROM (start_time - request_time)) / 60 <= 15 THEN 'до 15 хв (відповідає очікуванням)'
            WHEN EXTRACT(EPOCH FROM (start_time - request_time)) / 60 <= 45 THEN '15–45 хв (ще допустимо)'
            ELSE 'більше 45 хв (погано)'
        END AS sla_category
    FROM events
) AS sla_stats
GROUP BY sla_category;

-- 5. Середнє навантаження на модератора (запитів на день) 
WITH daily_requests_per_agent AS (
    SELECT 
        moderator,
        DATE(request_time) AS day,
        COUNT(*) AS requests_per_day
    FROM events
    GROUP BY moderator, DATE(request_time)
)
SELECT 
    ROUND(AVG(requests_per_day), 1) AS avg_requests_per_agent_per_day
FROM daily_requests_per_agent;

-- 6. Середній час очікування по кожному модератору
SELECT
    moderator,
    COUNT(*) AS total_requests,
    ROUND(AVG(EXTRACT(EPOCH FROM (start_time - request_time)) / 60), 2) AS avg_wait_time_min
FROM events
GROUP BY moderator
ORDER BY avg_wait_time_min ASC
LIMIT 15;

-- 7. Розподіл запитів за годинами доби
SELECT
    EXTRACT(HOUR FROM request_time) AS hour_of_day,
    COUNT(*) AS total_requests
FROM events
GROUP BY hour_of_day
ORDER BY 2 DESC;

-- 8. Розподіл запитів за днями тиждня
SELECT
    TO_CHAR(request_time, 'Day') AS day_of_week,
    COUNT(*) AS total_requests
FROM events
GROUP BY day_of_week
ORDER BY MIN(DATE_PART('dow', request_time));

-- 9.  Оцінка навантаження на одного модератора
WITH per_day AS (
    SELECT
        moderator,
        DATE(request_time) AS day,
        COUNT(*) AS daily_requests
    FROM events
    GROUP BY moderator, DATE(request_time)
)
SELECT
    moderator,
    ROUND(AVG(daily_requests), 1) AS avg_requests_per_day
FROM per_day
GROUP BY moderator
ORDER BY avg_requests_per_day DESC;

-- 10. Рекомендована кількість агентів на день (на основі обсягу запитів)
WITH daily_requests AS (
    SELECT 
        DATE(request_time) AS day,
        TO_CHAR(request_time, 'Day') AS weekday_name,
        COUNT(*) AS total_requests
    FROM events
    GROUP BY DATE(request_time), TO_CHAR(request_time, 'Day')
),
weekday_summary AS (
    SELECT 
        TRIM(weekday_name) AS weekday,
        ROUND(AVG(total_requests), 2) AS avg_requests_per_day,
        CEIL(AVG(total_requests) / 22.0) AS recommended_moderators
    FROM daily_requests
    GROUP BY TRIM(weekday_name)
)
SELECT * 
FROM weekday_summary
ORDER BY 
  CASE 
    WHEN weekday = 'Monday' THEN 1
    WHEN weekday = 'Tuesday' THEN 2
    WHEN weekday = 'Wednesday' THEN 3
    WHEN weekday = 'Thursday' THEN 4
    WHEN weekday = 'Friday' THEN 5
    WHEN weekday = 'Saturday' THEN 6
    WHEN weekday = 'Sunday' THEN 7
  END;

-- 11. Рекомендована кількість агентів на годину (на основі обсягу запитів)
WITH hourly_requests AS (
    SELECT 
        EXTRACT(HOUR FROM request_time) AS hour_of_day,
        COUNT(*) AS total_requests
    FROM events
    GROUP BY EXTRACT(HOUR FROM request_time)
),
recommended_moderators_by_hour AS (
    SELECT
        hour_of_day,
        total_requests,
        CEIL(total_requests / 528.0) AS recommended_moderators
    FROM hourly_requests
)
SELECT *
FROM recommended_moderators_by_hour
ORDER BY hour_of_day;

-- 12.  Показники роботи служби підтримки по дням
SELECT
    request_time::date AS request_date,
    COUNT(*) AS total_requests,
    ROUND(AVG(EXTRACT(EPOCH FROM (start_time - request_time)) / 60), 2) AS avg_wait_time_min,
    COUNT(DISTINCT moderator) AS unique_moderators
FROM events
WHERE start_time IS NOT NULL 
  AND request_time IS NOT NULL 
  AND start_time >= request_time
GROUP BY request_date
ORDER BY 1 ;

-- 13. Визначення модераторів, що обробили найбільшу кількість запитів в межах допустимих 15 хвилин
SELECT
    moderator,
    COUNT(*) AS count_requests,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percent
FROM (
    SELECT
        moderator,
        CASE
            WHEN EXTRACT(EPOCH FROM (start_time - request_time)) / 60 <= 15 THEN 'до 15 хв (відповідає очікуванням)'
            WHEN EXTRACT(EPOCH FROM (start_time - request_time)) / 60 <= 45 THEN '15–45 хв (ще допустимо)'
            ELSE 'більше 45 хв (погано)'
        END AS sla_category
    FROM events
    WHERE request_time IS NOT NULL AND start_time IS NOT NULL
) AS sla_stats
WHERE sla_category = 'до 15 хв (відповідає очікуванням)'
GROUP BY moderator
ORDER BY count_requests DESC
LIMIT 10;