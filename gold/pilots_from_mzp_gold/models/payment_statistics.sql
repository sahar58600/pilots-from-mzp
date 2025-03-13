{{ config(
    materialized='table',
) }}

SELECT
    DISTINCT
    TO_CHAR(payment_date::timestamp with time zone, 'YYYY-MM-DD') AS payment_date1,
    TO_CHAR(payment_date::timestamp with time zone, 'Day') AS payment_day,
    TO_CHAR(payment_date::timestamp with time zone, 'Month') AS payment_month,
    TO_CHAR(payment_date::timestamp with time zone, 'YYYY') AS payment_year,
    COUNT(*) AS deals_count,
    SUM(amount) AS deals_sum_amount,
    AVG(amount) AS deals_average_amount,
    MAX(amount) AS highest_deal,
    MIN(amount) AS lowest_deal
FROM
    silver.payments AS payments
GROUP BY
    payment_date1,
    payment_day,
    payment_month,
    payment_year
ORDER BY
    payment_date1 ASC