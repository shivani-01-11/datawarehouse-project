-- ============================================================
-- File Name : bronze_pax_passengers_data_validation_and_profiling.sql
-- Purpose   : Validate and profile bronze.pax_passengers data
--             before loading into silver layer
-- ============================================================


-- ============================================================
-- Check for duplicate or null passenger IDs
-- passenger_id is expected to identify a passenger, but duplicates exist
-- ============================================================
SELECT 
    passenger_id,
    COUNT(*) AS duplicate_count
FROM bronze.pax_passengers
GROUP BY passenger_id
HAVING COUNT(*) > 1
   OR passenger_id IS NULL;


-- ============================================================
-- Check for duplicate or null loyalty numbers
-- A passenger may have multiple bookings with same loyalty number
-- ============================================================
SELECT 
    loyalty_number,
    COUNT(*) AS duplicate_count
FROM bronze.pax_passengers
GROUP BY loyalty_number
HAVING COUNT(*) > 1
   OR loyalty_number IS NULL;


-- ============================================================
-- Check for possible duplicate passenger records
-- Since passport numbers are masked, include first and last name
-- ============================================================
SELECT 
    passenger_passport_number,
    first_name,
    last_name,
    COUNT(passenger_passport_number) AS duplicate_count
FROM bronze.pax_passengers
GROUP BY passenger_passport_number, first_name, last_name
HAVING COUNT(passenger_passport_number) > 1
   OR passenger_passport_number IS NULL;


-- ============================================================
-- Check first_name values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT first_name
FROM bronze.pax_passengers
WHERE first_name != TRIM(first_name);


-- ============================================================
-- Check last_name values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT last_name
FROM bronze.pax_passengers
WHERE last_name != TRIM(last_name);


-- ============================================================
-- Check nationality values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT nationality
FROM bronze.pax_passengers
WHERE nationality != TRIM(nationality);


-- ============================================================
-- Validate date_of_birth values
-- Date of birth should not be before 1900 or in the future
-- ============================================================
SELECT DISTINCT date_of_birth
FROM bronze.pax_passengers
WHERE date_of_birth < '1900-01-01'
   OR date_of_birth > CURRENT_DATE();


-- ============================================================
-- Check gender values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT gender
FROM bronze.pax_passengers
WHERE gender != TRIM(gender);


-- ============================================================
-- Review distinct gender values and planned transformation
-- M -> Male
-- F -> Female
-- Others -> n/a
-- ============================================================
SELECT DISTINCT
    gender,
    CASE
        WHEN UPPER(TRIM(gender)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(gender)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS standardized_gender
FROM bronze.pax_passengers;


-- ============================================================
-- Check seat_number values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT seat_number
FROM bronze.pax_passengers
WHERE seat_number != TRIM(seat_number);


-- ============================================================
-- Review distinct travel class values
-- Expected values: Economy | Business | First
-- ============================================================
SELECT DISTINCT travel_class
FROM bronze.pax_passengers;


-- ============================================================
-- Review distinct flight numbers
-- flight_number should match flight_id from ops_flights
-- ============================================================
SELECT DISTINCT flight_number
FROM bronze.pax_passengers;


-- ============================================================
-- Check for passenger records with invalid flight numbers
-- flight_number should exist in ops_flights
-- ============================================================
SELECT DISTINCT flight_number
FROM bronze.pax_passengers
WHERE flight_number NOT IN (
    SELECT DISTINCT flight_id
    FROM bronze.ops_flights
);


-- ============================================================
-- Validate check-in time and boarding time sequence
-- Boarding time should always occur after check-in time
-- Expected result: 0 records
-- ============================================================
SELECT 
    checkin_time,
    boarding_time
FROM bronze.pax_passengers
WHERE checkin_time > boarding_time;


-- ============================================================
-- Review no_show_flag values
-- Expected values: TRUE | FALSE
-- ============================================================
SELECT DISTINCT no_show_flag
FROM bronze.pax_passengers;


-- ============================================================
-- Compare passenger_age with age calculated from date_of_birth
-- Helps identify inaccurate age values
-- ============================================================
SELECT 
    date_of_birth,
    passenger_age
FROM bronze.pax_passengers
WHERE passenger_age = TIMESTAMPDIFF(
    YEAR,
    date_of_birth,
    CURRENT_DATE()
);


-- ============================================================
-- Recalculate passenger age where source age is incorrect
-- Negative age or mismatch should be corrected in silver layer
-- ============================================================
SELECT 
    date_of_birth,
    passenger_age,
    CASE
        WHEN passenger_age != TIMESTAMPDIFF(
            YEAR,
            date_of_birth,
            CURRENT_DATE()
        )
        THEN TIMESTAMPDIFF(
            YEAR,
            date_of_birth,
            CURRENT_DATE()
        )
        ELSE passenger_age
    END AS corrected_passenger_age
FROM bronze.pax_passengers;


-- ============================================================
-- Review satisfaction score values
-- Helps identify invalid or negative values
-- ============================================================
SELECT DISTINCT satisfaction_score
FROM bronze.pax_passengers;


-- ============================================================
-- Review distinct age group values
-- ============================================================
SELECT DISTINCT age_group
FROM bronze.pax_passengers;


-- ============================================================
-- Check for age group inconsistencies
-- Example: Child with very high age value
-- ============================================================
SELECT 
    age_group,
    passenger_age
FROM bronze.pax_passengers
WHERE age_group = 'child'
ORDER BY passenger_age DESC;


-- ============================================================
-- Recalculate age group using age derived from date_of_birth
-- Source age_group values are inconsistent
-- ============================================================
SELECT 
    date_of_birth,

    CASE
        WHEN passenger_age != TIMESTAMPDIFF(
            YEAR,
            date_of_birth,
            CURRENT_DATE()
        )
        THEN TIMESTAMPDIFF(
            YEAR,
            date_of_birth,
            CURRENT_DATE()
        )
        ELSE passenger_age
    END AS corrected_passenger_age,

    CASE
        WHEN TIMESTAMPDIFF(
            YEAR,
            date_of_birth,
            CURRENT_DATE()
        ) BETWEEN 1 AND 12 THEN 'Child'

        WHEN TIMESTAMPDIFF(
            YEAR,
            date_of_birth,
            CURRENT_DATE()
        ) BETWEEN 13 AND 24 THEN 'Youth'

        WHEN TIMESTAMPDIFF(
            YEAR,
            date_of_birth,
            CURRENT_DATE()
        ) BETWEEN 25 AND 64 THEN 'Adult'

        ELSE 'Senior'
    END AS recalculated_age_group
FROM bronze.pax_passengers;


-- ============================================================
-- Identify sparse columns that contain mostly NULL values
-- These columns may be dropped from silver layer
-- ============================================================
SELECT
    COUNT(special_meal_request) AS special_meal_request_count,
    COUNT(wheelchair_assistance_flag) AS wheelchair_assistance_flag_count,
    COUNT(unaccompanied_minor_flag) AS unaccompanied_minor_flag_count,
    COUNT(frequent_flyer_tier) AS frequent_flyer_tier_count,
    COUNT(passport_expiry_date) AS passport_expiry_date_count
FROM bronze.pax_passengers;