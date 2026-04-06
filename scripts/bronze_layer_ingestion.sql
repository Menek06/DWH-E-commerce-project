----------------------------------------------------
-- Creating and ingesting data to customers table --
----------------------------------------------------

DROP TABLE IF EXISTS bronze.customers;
CREATE TABLE IF NOT EXISTS bronze.customers(
	customer_id VARCHAR,
	first_name VARCHAR,
	last_name VARCHAR,
	cust_email VARCHAR,
	phone VARCHAR,
	city VARCHAR,
	country VARCHAR,
	registration_date VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW()
);

COPY bronze.customers
FROM '/DWH_PROJECT/source/customers.csv'
DELIMITER ','
CSV HEADER;

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

COPY bronze.order_items
FROM '/DWH_PROJECT/source/order_items.csv'
DELIMITER ','
CSV HEADER;

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

COPY bronze.orders
FROM '/DWH_PROJECT/source/orders.csv'
DELIMITER ','
CSV HEADER;

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

COPY bronze.payments
FROM '/DWH_PROJECT/source/payments.csv'
DELIMITER ','
CSV HEADER;

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

COPY bronze.products
FROM '/DWH_PROJECT/source/products.csv'
DELIMITER ','
CSV HEADER;


