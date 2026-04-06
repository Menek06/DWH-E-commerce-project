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

SELECT COUNT(*) AS number_of_rows FROM bronze.customers;