-- Active: 1775758877245@@127.0.0.1@5432@DWH_E-commarce
-- Transforming customer table, validating data types, cleaning white spaces, replaceing missing data 

-----------------------------------------------
-- customers table cleaning and transforming --
-----------------------------------------------
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


--------------------
-- order_items 
--------------------
-- deduplicate by item_id
-- null in discount changed to 0.00

DROP TABLE IF EXISTS silver.order_items;
CREATE TABLE IF NOT EXISTS silver.order_items (
    item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(3,2),
    _transformed_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO silver.order_items 
SELECT
	item_id,
	order_id,
	product_id,
	quantity,
	unit_price,
	discount
FROM (
	SELECT 
		item_id::INT,
		oi.order_id::INT,
		oi.product_id::INT,
		oi.quantity::INT,
		unit_price::DECIMAL(10,2),
		CASE 
			WHEN discount IS NULL THEN 0.00
			ElSE discount::DECIMAL(3,2)
		END AS discount,
		ROW_NUMBER() OVER(PARTITION BY item_id ORDER BY unit_price DESC) AS rn
	FROM bronze.order_items oi
	JOIN bronze.products p
	  ON oi.product_id = p.product_id
	JOIN bronze.orders o
	  ON o.order_id = oi.order_id
	WHERE quantity::INT > 0
	  AND item_id IS NOT NULL
)t
WHERE rn = 1;
INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'silver', 'order_items', COUNT(*) FROM silver.order_items;
