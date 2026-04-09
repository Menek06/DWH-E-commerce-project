-- Active: 1775758877245@@127.0.0.1@5432@DWH_E-commarce
-- Transforming customer table, validating data types, cleaning white spaces, replaceing missing data 
DROP TABLE IF EXISTS silver.customers;
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id       INT,
    first_name        VARCHAR,
    last_name         VARCHAR,
    email             VARCHAR,
    phone             VARCHAR,
    city              VARCHAR,
    country           VARCHAR,
    registration_date DATE,
    _transformed_at   TIMESTAMP DEFAULT NOW()
);
INSERT INTO silver.customers 
SELECT
	customer_id,
	first_name,
	last_name,
	email,
	phone,
	city,
	country,
	registration_date
FROM (
	SELECT 
		customer_id::int,
		TRIM(INITCAP(first_name)) AS first_name,
		TRIM(INITCAP(last_name)) AS last_name,
		CASE 
			WHEN email IS NULL THEN 'N/A'
			WHEN POSITION('@' in email) = 0 THEN 'N/A'
			ELSE email
		END AS email,
		COALESCE(phone, 'N/A') as phone,
		TRIM(INITCAP(city)) AS city,
		INITCAP(LOWER(TRIM(country))) AS country,
		CASE 
			WHEN POSITION('/' IN registration_date) > 0 THEN TO_DATE(registration_date, 'DD/MM/YYYY')
			WHEN POSITION('/' IN registration_date) = 0 THEN TO_DATE(registration_date, 'YYYY-MM-DD')
		END AS registration_date,
		ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY registration_date DESC) AS rn
	FROM bronze.customers AS c
	WHERE customer_id IS NOT NULL
)t
WHERE rn = 1;

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'silver', 'customers', COUNT(*) FROM silver.customers;

SELECT * FROM logs.etl_log

