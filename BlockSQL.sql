CREATE DATABASE ecommerce_data;


USE ecommerce_data;


-- Connecting CSV to Mysql 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customer_info.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Id_client, Total_amount, Gender, @Age, Count_city, Response_communcation, Communication_3month, Tenure)
SET Age = NULLIF(@Age, '');


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions_info.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@date_new, Id_check, ID_client, Count_products, Sum_payment)
SET date_new = STR_TO_DATE(@date_new, '%d/%m/%Y');


-- Question 1 

WITH ContinuousClients AS (
    -- Шаг 1: Находим ID клиентов, у которых было ровно 12 уникальных месяцев с транзакциями за год
    SELECT
        ID_client
    FROM
        transactions
    WHERE
        date_new >= '2015-06-01' AND date_new < '2016-06-01'
    GROUP BY
        ID_client
    HAVING
        COUNT(DISTINCT DATE_FORMAT(date_new, '%Y-%m')) = 12
)
-- Шаг 2: Рассчитываем метрики для этих клиентов
SELECT
    c.ID_client,
    SUM(t.Sum_payment) / COUNT(DISTINCT t.Id_check) AS avg_check_amount,
    SUM(t.Sum_payment) / 12 AS avg_monthly_spend,
    COUNT(t.Id_check) AS total_transactions_count
FROM
    transactions t
JOIN
    ContinuousClients c ON t.ID_client = c.ID_client
WHERE
    t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
GROUP BY
    c.ID_client
ORDER BY
    c.ID_client;




#2 a,b,c,d 

WITH MonthlyStats AS (
    -- Шаг 1: Собираем базовую статистику по каждому месяцу
    SELECT
        DATE_FORMAT(date_new, '%Y-%m') AS transaction_month,
        SUM(Sum_payment) AS monthly_sum,
        COUNT(*) AS monthly_transactions,
        COUNT(DISTINCT ID_client) AS monthly_clients_count,
        SUM(Sum_payment) / COUNT(DISTINCT Id_check) AS avg_check_per_month
    FROM
        transactions
    WHERE
        date_new >= '2015-06-01' AND date_new < '2016-06-01'
    GROUP BY
        transaction_month
),
TotalStats AS (
    -- Шаг 2: Рассчитываем общие показатели за год для вычисления долей
    SELECT
        SUM(Sum_payment) AS total_sum_for_year,
        COUNT(*) AS total_transactions_for_year
    FROM
        transactions
    WHERE
        date_new >= '2015-06-01' AND date_new < '2016-06-01'
)
-- Шаг 3: Объединяем и выводим финальный результат
SELECT
    ms.transaction_month,
    ms.avg_check_per_month,
    ms.monthly_transactions,
    ms.monthly_clients_count,
    (ms.monthly_transactions / ts.total_transactions_for_year) * 100 AS transaction_share_percent,
    (ms.monthly_sum / ts.total_sum_for_year) * 100 AS sum_share_percent
FROM
    MonthlyStats ms, TotalStats ts
ORDER BY
    ms.transaction_month;
    
# Question 2 e 
SELECT
    transaction_month,
    gender,
    COUNT(DISTINCT ID_client) AS unique_clients,
    SUM(Sum_payment) AS total_spend,
    -- Расчет доли клиентов каждого пола в общем числе клиентов за месяц
    (COUNT(DISTINCT ID_client) * 100.0) / SUM(COUNT(DISTINCT ID_client)) OVER(PARTITION BY transaction_month) AS client_percentage,
    -- Расчет доли трат каждого пола в общей сумме трат за месяц
    (SUM(Sum_payment) * 100.0) / SUM(SUM(Sum_payment)) OVER(PARTITION BY transaction_month) AS spend_percentage
FROM (
    SELECT
        t.ID_client,
        t.Sum_payment,
        DATE_FORMAT(t.date_new, '%Y-%m') AS transaction_month,
        COALESCE(c.Gender, 'NA') AS gender -- Заменяем NULL на 'NA'
    FROM
        transactions t
    LEFT JOIN
        customers c ON t.ID_client = c.Id_client
    WHERE
        t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
) AS SubQuery
GROUP BY
    transaction_month,
    gender
ORDER BY
    transaction_month,
    gender;
    
    
# Question 3 

SELECT
    CASE
        WHEN c.Age BETWEEN 0 AND 19 THEN '0-19'
        WHEN c.Age BETWEEN 20 AND 29 THEN '20-29'
        WHEN c.Age BETWEEN 30 AND 39 THEN '30-39'
        WHEN c.Age BETWEEN 40 AND 49 THEN '40-49'
        WHEN c.Age BETWEEN 50 AND 59 THEN '50-59'
        WHEN c.Age >= 60 THEN '60+'
        ELSE 'NA' -- Группа для клиентов без указания возраста
    END AS age_group,
    SUM(t.Sum_payment) AS total_sum,
    COUNT(*) AS total_transactions
FROM
    transactions t
LEFT JOIN
    customers c ON t.ID_client = c.Id_client
WHERE
    t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
GROUP BY
    age_group
ORDER BY
    age_group;
    
/* 0-19	129283.16	13957
20-29	740584.29	76997
30-39	711863.77	75217
40-49	476448.28	47513
50-59	508949.38	53215
60+	989828.61	108587
NA	56879.72	5816*/ 

-- Quarterly 
WITH QuarterlyData AS (
    SELECT
        CONCAT(YEAR(t.date_new), '-Q', QUARTER(t.date_new)) AS transaction_quarter,
        CASE
            WHEN c.Age BETWEEN 0 AND 19 THEN '0-19'
            WHEN c.Age BETWEEN 20 AND 29 THEN '20-29'
            WHEN c.Age BETWEEN 30 AND 39 THEN '30-39'
            WHEN c.Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN c.Age BETWEEN 50 AND 59 THEN '50-59'
            WHEN c.Age >= 60 THEN '60+'
            ELSE 'NA'
        END AS age_group,
        SUM(t.Sum_payment) AS group_quarterly_sum,
        COUNT(*) AS group_quarterly_transactions
    FROM
        transactions t
    LEFT JOIN
        customers c ON t.ID_client = c.Id_client
    WHERE
        t.date_new >= '2015-06-01' AND t.date_new < '2016-06-01'
    GROUP BY
        transaction_quarter, age_group
)
SELECT
    qd.transaction_quarter,
    qd.age_group,
    qd.group_quarterly_sum,
    qd.group_quarterly_transactions,
    -- Расчет доли трат и транзакций группы в рамках своего квартала
    (qd.group_quarterly_sum * 100.0) / SUM(qd.group_quarterly_sum) OVER(PARTITION BY qd.transaction_quarter) AS quarterly_sum_percentage,
    (qd.group_quarterly_transactions * 100.0) / SUM(qd.group_quarterly_transactions) OVER(PARTITION BY qd.transaction_quarter) AS quarterly_transaction_percentage
FROM
    QuarterlyData qd
ORDER BY
    qd.transaction_quarter, qd.age_group;