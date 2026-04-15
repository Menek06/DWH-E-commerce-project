CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.customers(
	customer_id VARCHAR,
	first_name VARCHAR,
	last_name VARCHAR,
	email VARCHAR,
	phone VARCHAR,
	city VARCHAR,
	country VARCHAR,
	registration_date VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW(),
	_source_file VARCHAR DEFAULT 'customers.csv'
);

CREATE TABLE IF NOT EXISTS bronze.order_items(
	item_id varchar,
	order_id VARCHAR,
	product_id VARCHAR,
	quantity VARCHAR,
	unit_price VARCHAR,
	discount VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW(),
	_source_file VARCHAR DEFAULT 'order_items.csv'
);

CREATE TABLE IF NOT EXISTS bronze.orders(
	order_id VARCHAR,
	customer_id VARCHAR,
	order_date VARCHAR,
	status VARCHAR,
	shipping_address VARCHAR,
	shipping_city VARCHAR,
	shipping_country VARCHAR,
	total_amount VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW(),
	_source_file VARCHAR DEFAULT 'orders.csv'
);

CREATE TABLE IF NOT EXISTS bronze.payments(
	payment_id VARCHAR,
	order_id VARCHAR,
	payment_date VARCHAR,
	payment_method VARCHAR,
	amount VARCHAR,
	transaction_id VARCHAR,
	status VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW(),
	_source_file VARCHAR DEFAULT 'payments.csv'
);

CREATE TABLE IF NOT EXISTS bronze.products(
	product_id VARCHAR,
	product_name VARCHAR,
	category VARCHAR,
	price VARCHAR,
	stock VARCHAR,
	weight_kg VARCHAR,
	description VARCHAR,
	created_at VARCHAR,
	_loaded_at TIMESTAMP DEFAULT NOW(),
	_source_file VARCHAR DEFAULT 'products.csv'
);