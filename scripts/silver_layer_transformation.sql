-- Active: 1775758877245@@127.0.0.1@5432@DWH_E-commarce
-- Transforming customer table, validating data types, cleaning white spaces, replaceing missing data 
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
	END AS registration_date
FROM bronze.customers AS c;

--Select  * FROM bronze.customers
