
BEGIN;

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
    phone 			  VARCHAR,
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
    item_id         INT,
    order_id        INT,
    product_id      INT,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(3,2),
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


------------------
-- orders table --
------------------

DROP TABLE IF EXISTS silver.orders;
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id         INT,
    customer_id      INT,
    order_date       DATE,
    status           VARCHAR(255),
    shipping_address VARCHAR(255),
    shipping_city    VARCHAR(255),
	shipping_country VARCHAR(255),
	total_amount     DECIMAL(10,2),
    _transformed_at  TIMESTAMP DEFAULT NOW()
);
INSERT INTO silver.orders 
SELECT 
	order_id,
	customer_id,
	order_date,
	status,
	shipping_address,
	shipping_city,
	shipping_country,
	total_amount
FROM (
	SELECT
		order_id::INT,
		o.customer_id::INT,
		CASE 
			WHEN POSITION('/' IN order_date) > 0 THEN TO_DATE(order_date, 'DD/MM/YYYY')
			WHEN POSITION('/' IN order_date) = 0 THEN TO_DATE(order_date, 'YYYY-MM-DD')
		END AS order_date,
		TRIM(LOWER(status)) AS status,
		COALESCE(TRIM(shipping_address), 'N/A') AS shipping_address,
		COALESCE(TRIM(INITCAP(shipping_city)), 'N/A') AS shipping_city,
		COALESCE(TRIM(INITCAP(shipping_country)), 'N/A') AS shipping_country,
		total_amount::DECIMAL(10,2),
		ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY order_date, order_id) AS rn
	FROM bronze.orders o
	JOIN bronze.customers c
	  ON c.customer_id = o.customer_id
	WHERE order_id IS NOT NULL
	 AND total_amount::DECIMAL(10,2) > 0
) AS t
WHERE rn = 1;
INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'silver', 'orders', COUNT(*) FROM silver.orders;



--------------
-- payments --
--------------


DROP TABLE IF EXISTS silver.payments;
CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id       INT,
    order_id      	 INT,
    payment_date     DATE,
    payment_method   VARCHAR(255),
    amount 			 DECIMAL(10,2),
    transaction_id   VARCHAR(255),
	status 			 VARCHAR(255),
    _transformed_at  TIMESTAMP DEFAULT NOW()
);
INSERT INTO silver.payments 
SELECT
	payment_id,
	order_id,
	payment_date,
	payment_method,
	amount,
	transaction_id,
	status
FROM(
	SELECT
		payment_id::INT,
		p.order_id::INT,
		CASE 
			WHEN POSITION('/' IN payment_date) > 0 THEN TO_DATE(payment_date, 'DD/MM/YYYY')
			WHEN POSITION('/' IN payment_date) = 0 THEN TO_DATE(payment_date, 'YYYY-MM-DD')
		END AS payment_date,
		LOWER(TRIM(REPLACE(payment_method, ' ', '_'))) AS payment_method,
		amount::DECIMAL(10,2),
		COALESCE(transaction_id, 'N/A') AS transaction_id,
		LOWER(TRIM(p.status)) AS status,
		ROW_NUMBER() OVER(PARTITION BY payment_id ORDER BY payment_date) AS rn
	FROM bronze.payments p
	JOIN silver.orders o
	  ON p.order_id::INT = o.order_id
	WHERE payment_id IS NOT NULL
)t
WHERE rn = 1
  AND amount > 0;

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'silver', 'payments', COUNT(*) FROM silver.payments;


-----------------
--   products  --
-----------------

DROP TABLE IF EXISTS silver.products;
CREATE TABLE IF NOT EXISTS silver.products (
	product_id       INT,
	product_name     VARCHAR(255),
	category         VARCHAR(255),
	price            DECIMAL(10, 2),
	stock            INT,
	weight_kg        DECIMAL(10, 2),
	description      VARCHAR,
	created_at 		 DATE,
    _transformed_at  TIMESTAMP DEFAULT NOW()
);
INSERT INTO silver.products 
SELECT
	product_id,
	product_name,
	category,
	price,
	stock,
	weight_kg,
	description,
	created_at
FROM (
	SELECT 
		product_id::INT,
		TRIM(product_name) AS product_name,
		INITCAP(category) AS category,
		price::DECIMAL(10, 2),
		stock::INT,
		weight_kg::DECIMAL(10, 2),
		COALESCE(description, 'N/A') AS description,
		CASE 
			WHEN POSITION('/' IN created_at) > 0 THEN TO_DATE(created_at, 'DD/MM/YYYY')
			WHEN POSITION('/' IN created_at) = 0 THEN TO_DATE(created_at, 'YYYY-MM-DD')
		END AS created_at,
		ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY  created_at) AS rn
	FROM bronze.products
	WHERE product_id IS NOT NULL 
	  AND price::DECIMAL > 0
)t
WHERE rn = 1;
  

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'silver', 'products', COUNT(*) FROM silver.products;



SELECT 'silver.customers'   AS tabela, COUNT(*) AS rekordy FROM silver.customers
UNION ALL
SELECT 'silver.products'    AS tabela, COUNT(*) AS rekordy FROM silver.products
UNION ALL
SELECT 'silver.orders'      AS tabela, COUNT(*) AS rekordy FROM silver.orders
UNION ALL
SELECT 'silver.order_items' AS tabela, COUNT(*) AS rekordy FROM silver.order_items
UNION ALL
SELECT 'silver.payments'    AS tabela, COUNT(*) AS rekordy FROM silver.payments
ORDER BY tabela;

COMMIT;