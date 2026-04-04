-- ============================================================
-- File Name : bronze_pax_passengers_data_validation_and_profiling.sql
-- Purpose   : Create and load silver.pax_passengers table
--             from bronze.pax_passengers with cleansing,
--             standardization and transformation rules
-- ============================================================


USE aviation_dw;


-- ============================================================
-- 2. Create silver.pax_passengers
-- ============================================================
-- Notes:
-- 1. Surrogate key added.
-- 2. Sparse / unused columns removed from silver layer:
--      - special_meal_request
--      - wheelchair_assistance_flag
--      - unaccompanied_minor_flag
--      - frequent_flyer_tier
--      - passport_expiry_date
-- 3. gender values standardized
-- 4. age_group recalculated from age
-- 5. booking_channel corrected where contaminated
-- ============================================================

DROP TABLE IF EXISTS silver.pax_passengers;

CREATE TABLE silver.pax_passengers (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    passenger_id                VARCHAR(20),
    loyalty_number              VARCHAR(30),
    passenger_passport_number   VARCHAR(30),
    first_name                  VARCHAR(100),
    last_name                   VARCHAR(100),
    nationality                 VARCHAR(100),
    date_of_birth               DATE,
    gender                      VARCHAR(10),      -- Male | Female | n/a
    seat_number                 VARCHAR(10),
    travel_class                VARCHAR(30),      -- Economy | Business | First
    flight_number               VARCHAR(20),
    checkin_time                DATETIME,
    boarding_time               DATETIME,
    gate_number                 VARCHAR(10),
    baggage_count               INT,
    email_address               VARCHAR(255),
    phone_number                VARCHAR(30),
    no_show_flag                TINYINT,
    satisfaction_score          DECIMAL(4,2),
    vip_flag                    TINYINT,
    booking_channel             VARCHAR(50),
    passenger_age               INT,
    age_group                   VARCHAR(30),
    dwh_create_date             DATETIME DEFAULT NOW()
);

-- ============================================================
-- Load silver.pax_passengers from bronze.pax_passengers

-- ============================================================
-- Key Transformations Applied While Loading To Silver Layer
-- ============================================================

-- 1. Added surrogate primary key column: id

-- 2. Removed sparse / unused columns:
--      special_meal_request
--      wheelchair_assistance_flag
--      unaccompanied_minor_flag
--      frequent_flyer_tier
--      passport_expiry_date

-- 3. Standardized gender values:
--      M -> Male
--      F -> Female
--      Other / blank -> n/a

-- 4. Standardized checkin_time and boarding_time to DATETIME

-- 5. Converted no_show_flag and vip_flag to numeric boolean values:
--      TRUE -> 1
--      FALSE -> 0

-- 6. Corrected booking_channel values where source contains travel class names

-- 7. Recalculated passenger_age using date_of_birth when source age is invalid

-- 8. Recalculated age_group using corrected age:
--      1 - 12   -> Child
--      13 - 24  -> Youth
--      25 - 64  -> Adult
--      65+      -> Senior

-- 9. Converted invalid negative satisfaction_score values to NULL
-- ============================================================


SELECT '>> Truncating Table: silver.pax_passengers' AS load_log;
TRUNCATE TABLE silver.pax_passengers;

SELECT '>> Inserting Data Into: silver.pax_passengers' AS load_log;

INSERT INTO silver.pax_passengers (
    passenger_id,
    loyalty_number,
    passenger_passport_number,
    first_name,
    last_name,
    nationality,
    date_of_birth,
    gender,
    seat_number,
    travel_class,
    flight_number,
    checkin_time,
    boarding_time,
    gate_number,
    baggage_count,
    email_address,
    phone_number,
    no_show_flag,
    satisfaction_score,
    vip_flag,
    booking_channel,
    passenger_age,
    age_group
)
SELECT
    TRIM(passenger_id),

    -- Loyalty number may repeat across records if passenger booked multiple flights
    NULLIF(TRIM(loyalty_number), ''),

    TRIM(passenger_passport_number),

    -- Remove leading / trailing spaces from names
    TRIM(first_name),
    TRIM(last_name),

    TRIM(nationality),

    -- Convert date string to DATE datatype
    STR_TO_DATE(date_of_birth, '%Y-%m-%d'),

    -- Standardize gender values
    CASE
        WHEN UPPER(TRIM(gender)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(gender)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS gender,

    TRIM(seat_number),

    -- Keep travel class values standardized
    TRIM(travel_class),

    -- Same as flight_id in ops_flights
    TRIM(flight_number),

    -- Source contains microseconds; keep only first 19 chars before conversion
    STR_TO_DATE(LEFT(checkin_time, 19), '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(LEFT(boarding_time, 19), '%Y-%m-%d %H:%i:%s'),

    UPPER(TRIM(gate_number)),

    CAST(baggage_count AS SIGNED),

    TRIM(email_address),
    TRIM(phone_number),

    -- Convert no_show flag to 1 / 0
    CASE
        WHEN UPPER(TRIM(no_show_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS no_show_flag,

    -- Invalid negative scores converted to NULL
    CASE
        WHEN CAST(satisfaction_score AS DECIMAL(6,2)) < 0 THEN NULL
        ELSE ROUND(CAST(satisfaction_score AS DECIMAL(4,2)), 2)
    END AS satisfaction_score,

    -- Convert VIP flag to 1 / 0
    CASE
        WHEN UPPER(TRIM(vip_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS vip_flag,

    -- booking_channel column contains travel class values in some rows
    -- Replace invalid values with 'n/a'
    CASE
        WHEN TRIM(booking_channel) IN ('Economy', 'Business', 'First') THEN 'n/a'
        WHEN NULLIF(TRIM(booking_channel), '') IS NULL THEN 'n/a'
        ELSE TRIM(booking_channel)
    END AS booking_channel,

    -- Passenger age recalculated if source age is invalid
    CASE
        WHEN CAST(passenger_age AS SIGNED) < 0 THEN TIMESTAMPDIFF(
            YEAR,
            STR_TO_DATE(date_of_birth, '%Y-%m-%d'),
            CURRENT_DATE()
        )
        WHEN CAST(passenger_age AS SIGNED) != TIMESTAMPDIFF(
            YEAR,
            STR_TO_DATE(date_of_birth, '%Y-%m-%d'),
            CURRENT_DATE()
        )
        THEN TIMESTAMPDIFF(
            YEAR,
            STR_TO_DATE(date_of_birth, '%Y-%m-%d'),
            CURRENT_DATE()
        )
        ELSE CAST(passenger_age AS SIGNED)
    END AS passenger_age,

    -- Recalculate age group because source labels are inconsistent
    CASE
        WHEN TIMESTAMPDIFF(
            YEAR,
            STR_TO_DATE(date_of_birth, '%Y-%m-%d'),
            CURRENT_DATE()
        ) BETWEEN 1 AND 12 THEN 'Child'

        WHEN TIMESTAMPDIFF(
            YEAR,
            STR_TO_DATE(date_of_birth, '%Y-%m-%d'),
            CURRENT_DATE()
        ) BETWEEN 13 AND 24 THEN 'Youth'

        WHEN TIMESTAMPDIFF(
            YEAR,
            STR_TO_DATE(date_of_birth, '%Y-%m-%d'),
            CURRENT_DATE()
        ) BETWEEN 25 AND 64 THEN 'Adult'

        ELSE 'Senior'
    END AS age_group

FROM bronze.pax_passengers
WHERE NULLIF(TRIM(passenger_id), '') IS NOT NULL;
