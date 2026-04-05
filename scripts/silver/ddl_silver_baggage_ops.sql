-- ============================================================
-- silver.ops_baggage
-- Source  : bronze.ops_baggage
-- Grain   : one row per baggage tag
-- Changes :
--   - Added surrogate primary key column: id
--   - Retained baggage_tag_number as business key
--   - baggage_weight_kg converted to DECIMAL
--   - terminal_number converted to INT
--   - booleans converted to TINYINT
--   - corrected baggage process timestamps applied
--   - baggage timestamps parsed from MM/DD/YY HH:MM format
-- ============================================================

DROP TABLE IF EXISTS silver.ops_baggage;

CREATE TABLE silver.ops_baggage (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    baggage_tag_number          VARCHAR(30),
    passenger_id                VARCHAR(20),
    flight_id                   VARCHAR(20),
    passenger_passport_number   VARCHAR(30),
    baggage_weight_kg           DECIMAL(8,4),
    baggage_dimensions_cm       VARCHAR(20),
    baggage_type                VARCHAR(30),
    checkin_counter             VARCHAR(10),

    -- Original source check-in timestamp
    checkin_time                DATETIME,

    -- Corrected baggage scan timestamp
    baggage_scan_time           DATETIME,

    terminal_number             INT,
    baggage_status              VARCHAR(30),
    oversized_baggage_flag      TINYINT,
    baggage_delay_minutes       INT,
    baggage_location            VARCHAR(50),

    -- Corrected baggage loaded timestamp
    baggage_loaded_time         DATETIME,

    damaged_baggage_flag        TINYINT,

    dwh_create_date             DATETIME DEFAULT NOW()
);



-- ============================================================
-- Load silver.ops_baggage from bronze.ops_baggage
-- ============================================================

TRUNCATE TABLE silver.ops_baggage;

SELECT '>> Inserting Data Into: silver.ops_baggage' AS load_log;

INSERT INTO silver.ops_baggage (
    baggage_tag_number,
    passenger_id,
    flight_id,
    passenger_passport_number,
    baggage_weight_kg,
    baggage_dimensions_cm,
    baggage_type,
    checkin_counter,
    checkin_time,
    baggage_scan_time,
    terminal_number,
    baggage_status,
    oversized_baggage_flag,
    baggage_delay_minutes,
    baggage_location,
    baggage_loaded_time,
    damaged_baggage_flag
)
SELECT
    TRIM(baggage_tag_number),

    TRIM(passenger_id),

    TRIM(flight_id),

    TRIM(passenger_passport_number),

    ROUND(CAST(baggage_weight_kg AS DECIMAL(8,4)), 4),

    TRIM(baggage_dimensions_cm),

    TRIM(baggage_type),

    UPPER(TRIM(checkin_counter)),

    -- Source timestamp format is MM/DD/YY HH:MM
    -- If source value is null or invalid, use current timestamp
    COALESCE(
        STR_TO_DATE(checkin_time, '%m/%d/%y %H:%i'),
        NOW()
    ) AS checkin_time,

    -- Generate realistic baggage scan timestamp
    -- Usually scan happens 5 to 30 minutes after check-in
    DATE_ADD(
        COALESCE(
            STR_TO_DATE(checkin_time, '%m/%d/%y %H:%i'),
            NOW()
        ),
        INTERVAL FLOOR(5 + RAND() * 25) MINUTE
    ) AS baggage_scan_time,

    CAST(terminal_number AS SIGNED),

    TRIM(baggage_status),

    CASE
        WHEN UPPER(TRIM(oversized_baggage_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS oversized_baggage_flag,

    CAST(baggage_delay_minutes AS SIGNED),

    TRIM(baggage_location),

    -- Generate realistic baggage loaded timestamp
    -- Usually baggage is loaded 10 to 60 minutes after scan
    DATE_ADD(
        DATE_ADD(
            COALESCE(
                STR_TO_DATE(checkin_time, '%m/%d/%y %H:%i'),
                NOW()
            ),
            INTERVAL FLOOR(5 + RAND() * 25) MINUTE
        ),
        INTERVAL FLOOR(10 + RAND() * 50) MINUTE
    ) AS baggage_loaded_time,

    CASE
        WHEN UPPER(TRIM(damaged_baggage_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS damaged_baggage_flag

FROM bronze.ops_baggage
WHERE NULLIF(TRIM(baggage_tag_number), '') IS NOT NULL;