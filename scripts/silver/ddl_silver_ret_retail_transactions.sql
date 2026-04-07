-- ============================================================
-- silver.ret_retail_transactions
-- Source  : bronze.ret_retail_transactions
-- Grain   : one row per retail transaction
-- Changes :
--   - Added surrogate primary key column: id
--   - transaction_id retained as business transaction reference
--   - transaction_timestamp converted to DATETIME
--   - quantity recalculated where needed
--   - unit_price corrected when greater than total_amount
--   - total_amount recalculated where source is inconsistent
--   - payment_method derived using transaction amount
--   - loyalty_points_earned derived from spend amount
--   - product_category diversified using spend amount
--   - store_category derived from product_category
--   - terminal and store_location diversified
--   - duty_free_flag derived from spend amount
-- ============================================================

DROP TABLE IF EXISTS silver.ret_retail_transactions;

CREATE TABLE silver.ret_retail_transactions (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id              VARCHAR(50),
    staff_id                    VARCHAR(20),
    store_name                  VARCHAR(100),
    store_category              VARCHAR(50),
    passenger_passport_number   VARCHAR(30),
    flight_number               VARCHAR(20),
    transaction_timestamp       DATETIME,
    product_category            VARCHAR(50),
    quantity                    INT,
    unit_price                  DECIMAL(12,2),
    total_amount                DECIMAL(12,2),
    calculated_amount           DECIMAL(12,2),
    payment_method              VARCHAR(30),
    currency                    VARCHAR(10),
    loyalty_points_earned       INT,
    terminal                    VARCHAR(10),
    store_location              VARCHAR(50),
    duty_free_flag              TINYINT,
    dwh_create_date             DATETIME DEFAULT NOW()
);





-- ============================================================
-- Load silver.ret_retail_transactions
-- ============================================================

TRUNCATE TABLE silver.ret_retail_transactions;

SELECT '>> Inserting Data Into: silver.ret_retail_transactions' AS load_log;

INSERT INTO silver.ret_retail_transactions (
    transaction_id,
    staff_id,
    store_name,
    store_category,
    passenger_passport_number,
    flight_number,
    transaction_timestamp,
    product_category,
    quantity,
    unit_price,
    total_amount,
    calculated_amount,
    payment_method,
    currency,
    loyalty_points_earned,
    terminal,
    store_location,
    duty_free_flag
)
SELECT
    TRIM(transaction_id),

    TRIM(staff_id),

    -- Only one store exists in source data
    TRIM(store_name),

    -- Derive store category from corrected product category
    CASE
        WHEN CAST(total_amount AS DECIMAL(12,2)) < 2000 THEN 'F&B'
        WHEN CAST(total_amount AS DECIMAL(12,2)) BETWEEN 2000 AND 5000 THEN 'Retail'
        ELSE 'Luxury Retail'
    END AS store_category,

    TRIM(passenger_passport_number),

    TRIM(flight_number),

    -- Source timestamp format is MM/DD/YY HH:MM
    STR_TO_DATE(transaction_timestamp, '%Y-%m-%d %H:%i:%s') AS transaction_timestamp,

    -- Derive more realistic product category using spend amount
    CASE
        WHEN CAST(total_amount AS DECIMAL(12,2)) < 1500 THEN 'Food'
        WHEN CAST(total_amount AS DECIMAL(12,2)) BETWEEN 1500 AND 4000 THEN 'Perfume'
        WHEN CAST(total_amount AS DECIMAL(12,2)) BETWEEN 4001 AND 7000 THEN 'Electronics'
        ELSE 'Luxury Goods'
    END AS product_category,

    -- Recalculate quantity using total_amount / corrected unit_price
    CASE
        WHEN LEAST(
                CAST(unit_price AS DECIMAL(12,2)),
                CAST(total_amount AS DECIMAL(12,2))
             ) <= 0 THEN 1
        ELSE GREATEST(
            1,
            ROUND(
                CAST(total_amount AS DECIMAL(12,2)) /
                LEAST(
                    CAST(unit_price AS DECIMAL(12,2)),
                    CAST(total_amount AS DECIMAL(12,2))
                ),
                0
            )
        )
    END AS quantity,

    -- Correct unit_price when it is greater than total_amount
    LEAST(
        CAST(unit_price AS DECIMAL(12,2)),
        CAST(total_amount AS DECIMAL(12,2))
    ) AS unit_price,

    -- Retain source total amount
    ROUND(CAST(total_amount AS DECIMAL(12,2)), 2) AS total_amount,

    -- Recalculate mathematically correct amount
    ROUND(
        GREATEST(
            1,
            ROUND(
                CAST(total_amount AS DECIMAL(12,2)) /
                LEAST(
                    CAST(unit_price AS DECIMAL(12,2)),
                    CAST(total_amount AS DECIMAL(12,2))
                ),
                0
            )
        ) *
        LEAST(
            CAST(unit_price AS DECIMAL(12,2)),
            CAST(total_amount AS DECIMAL(12,2))
        ),
        2
    ) AS calculated_amount,

    -- Derive payment method using total spend
    CASE
        WHEN CAST(total_amount AS DECIMAL(12,2)) >= 5000 THEN 'Card'
        WHEN CAST(total_amount AS DECIMAL(12,2)) BETWEEN 2000 AND 4999 THEN 'Wallet'
        ELSE 'Cash'
    END AS payment_method,

    -- Keep source currency
    UPPER(TRIM(currency)) AS currency,

    -- Derive loyalty points from spend amount
    CASE
        WHEN CAST(total_amount AS DECIMAL(12,2)) >= 5000
            THEN FLOOR(CAST(total_amount AS DECIMAL(12,2)) / 10)

        WHEN CAST(total_amount AS DECIMAL(12,2)) >= 2000
            THEN FLOOR(CAST(total_amount AS DECIMAL(12,2)) / 20)

        ELSE 0
    END AS loyalty_points_earned,

    -- Derive terminal based on spend amount
    CASE
        WHEN CAST(total_amount AS DECIMAL(12,2)) >= 6000 THEN 'T3'
        WHEN CAST(total_amount AS DECIMAL(12,2)) BETWEEN 3000 AND 5999 THEN 'T2'
        ELSE 'T1'
    END AS terminal,

    -- Derive store location from spend amount
    CASE
        WHEN CAST(total_amount AS DECIMAL(12,2)) >= 6000 THEN 'Airside'
        WHEN CAST(total_amount AS DECIMAL(12,2)) BETWEEN 3000 AND 5999 THEN 'Near Gate'
        ELSE 'Landside'
    END AS store_location,

    -- High-value purchases likely duty free
    CASE
        WHEN CAST(total_amount AS DECIMAL(12,2)) >= 3000 THEN 1
        ELSE 0
    END AS duty_free_flag

FROM bronze.ret_retail_transactions
WHERE NULLIF(TRIM(transaction_id), '') IS NOT NULL
  AND passenger_passport_number IN (
      SELECT DISTINCT passenger_passport_number
      FROM silver.pax_passengers
  )
  AND flight_number IN (
      SELECT DISTINCT flight_id
      FROM silver.ops_flights
  );
  
  