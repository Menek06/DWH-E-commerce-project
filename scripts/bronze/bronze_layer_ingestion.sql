CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time       TIMESTAMP;
    v_end_time         TIMESTAMP;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time   TIMESTAMP;
BEGIN
    v_batch_start_time := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    ---------------------------------------------------------------------------
    -- 1. CUSTOMERS
    ---------------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.customers';
    TRUNCATE TABLE bronze.customers;
    
    RAISE NOTICE '>> Inserting Data Into: bronze.customers';
    COPY bronze.customers(customer_id, first_name, last_name, email, phone, city, country, registration_date)
    FROM '/DWH_PROJECT/source/customers.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
    SELECT 'bronze', 'customers', COUNT(*) FROM bronze.customers;

    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '>> -------------';

    ---------------------------------------------------------------------------
    -- 2. ORDER ITEMS
    ---------------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.order_items';
    TRUNCATE TABLE bronze.order_items;

    RAISE NOTICE '>> Inserting Data Into: bronze.order_items';
    COPY bronze.order_items(item_id, order_id, product_id, quantity, unit_price, discount)
    FROM '/DWH_PROJECT/source/order_items.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
    SELECT 'bronze', 'order_items', COUNT(*) FROM bronze.order_items;

    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '>> -------------';

    ---------------------------------------------------------------------------
    -- 3. ORDERS
    ---------------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.orders';
    TRUNCATE TABLE bronze.orders;

    RAISE NOTICE '>> Inserting Data Into: bronze.orders';
    COPY bronze.orders(order_id, customer_id, order_date, status, shipping_address, shipping_city, shipping_country, total_amount)
    FROM '/DWH_PROJECT/source/orders.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
    SELECT 'bronze', 'orders', COUNT(*) FROM bronze.orders;

    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '>> -------------';

    ---------------------------------------------------------------------------
    -- 4. PAYMENTS
    ---------------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.payments';
    TRUNCATE TABLE bronze.payments;

    RAISE NOTICE '>> Inserting Data Into: bronze.payments';
    COPY bronze.payments(payment_id, order_id, payment_date, payment_method, amount, transaction_id, status)
    FROM '/DWH_PROJECT/source/payments.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
    SELECT 'bronze', 'payments', COUNT(*) FROM bronze.payments;

    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '>> -------------';

    ---------------------------------------------------------------------------
    -- 5. PRODUCTS
    ---------------------------------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.products';
    TRUNCATE TABLE bronze.products;

    RAISE NOTICE '>> Inserting Data Into: bronze.products';
    COPY bronze.products(product_id, product_name, category, price, stock, weight_kg, description, created_at)
    FROM '/DWH_PROJECT/source/products.csv'
    WITH (FORMAT CSV, HEADER true, DELIMITER ',');

    INSERT INTO logs.etl_log (layer, table_name, rows_loaded)
    SELECT 'bronze', 'products', COUNT(*) FROM bronze.products;

    v_end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    RAISE NOTICE '>> -------------';

    ---------------------------------------------------------------------------
    -- PODSUMOWANIE KOŃCOWE
    ---------------------------------------------------------------------------
    v_batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error State: %', SQLSTATE;
    RAISE NOTICE '==========================================';
    -- W procedurach PG błąd automatycznie wycofuje transakcję (rollback)
END;
$$;

CALL bronze.load_bronze();