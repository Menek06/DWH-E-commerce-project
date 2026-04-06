CREATE SCHEMA IF NOT EXISTS bronze;

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
	registration_date VARCHAR
);

COPY bronze.customers(customer_id, first_name, last_name, cust_email, phone, city , country, registration_date)
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
	discount VARCHAR
)

COPY bronze.order_items
FROM '/DWH_PROJECT/source/order_items.csv'
DELIMITER ','
CSV HEADER;


