BEGIN;
----------------------------------------------------
-- Creating and ingesting data to customers table --
----------------------------------------------------
DROP TABLE IF EXISTS bronze.customers;
CREATE TABLE IF NOT EXISTS bronze.customers(
	customer_id VARCHAR,
	first_name VARCHAR,
	last_name VARCHAR,
	email VARCHAR,
	phone VARCHAR,
	city VARCHAR,
	country VARCHAR,
	registration_date VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW()
);

COPY bronze.customers(customer_id, first_name, last_name, email, phone, city, country, registration_date)
FROM '/DWH_PROJECT/source/customers.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'bronze', 'customers', COUNT(*) FROM bronze.customers;

------------------------------------------------------
-- Creating and ingesting data to order items table --
------------------------------------------------------

DROP TABLE IF EXISTS bronze.order_items;
CREATE TABLE IF NOT EXISTS bronze.order_items(
	item_id varchar,
	order_id VARCHAR,
	product_id VARCHAR,
	quantity VARCHAR,
	unit_price VARCHAR,
	discount VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW()
);

COPY bronze.order_items(item_id, order_id, product_id, quantity, unit_price, discount)
FROM '/DWH_PROJECT/source/order_items.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'bronze', 'order_items', COUNT(*) FROM bronze.order_items;

--------------------------------------------------
-- Creating and ingesting data to orders table --
--------------------------------------------------

DROP TABLE IF EXISTS bronze.orders;
CREATE TABLE IF NOT EXISTS bronze.orders(
	order_id VARCHAR,
	customer_id VARCHAR,
	order_date VARCHAR,
	status VARCHAR,
	shipping_address VARCHAR,
	shipping_city VARCHAR,
	shipping_country VARCHAR,
	total_amount VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW()
);

COPY bronze.orders(order_id, customer_id, order_date, status, shipping_address, shipping_city, shipping_country, total_amount)
FROM '/DWH_PROJECT/source/orders.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'bronze', 'orders', COUNT(*) FROM bronze.orders;

----------------------------------------------------
-- Creating and ingesting data to payments table --
----------------------------------------------------

DROP TABLE IF EXISTS bronze.payments;
CREATE TABLE IF NOT EXISTS bronze.payments(
	payment_id VARCHAR,
	order_id VARCHAR,
	payment_date VARCHAR,
	payment_method VARCHAR,
	amount VARCHAR,
	transaction_id VARCHAR,
	status VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW()
);

COPY bronze.payments(payment_id, order_id, payment_date, payment_method, amount, transaction_id, status)
FROM '/DWH_PROJECT/source/payments.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'bronze', 'payments', COUNT(*) FROM bronze.payments;

----------------------------------------------------
-- Creating and ingesting data to products table --
----------------------------------------------------

DROP TABLE IF EXISTS bronze.products;
CREATE TABLE IF NOT EXISTS bronze.products(
	product_id VARCHAR,
	product_name VARCHAR,
	category VARCHAR,
	price VARCHAR,
	stock VARCHAR,
	weight_kg VARCHAR,
	description VARCHAR,
	created_at VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW()
);

COPY bronze.products(product_id, product_name, category, price, stock, weight_kg, description, created_at)
FROM '/DWH_PROJECT/source/products.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
SELECT 'bronze', 'products', COUNT(*) FROM bronze.products;

COMMIT;