-- ============================================================
-- File Name : bronze_ops_flights_data_validation_and_profiling.sql
-- Purpose   : Validate and profile raw flight data before
--             loading into silver layer
-- Table     : bronze.ops_flights
-- ============================================================

-- ============================================================
-- Check for duplicate or null flight IDs
-- Flight ID is expected to uniquely identify one flight leg
-- These records may need deduplication before silver load
-- ============================================================
SELECT 
    flight_id,
    COUNT(*) AS duplicate_count
FROM bronze.ops_flights
GROUP BY flight_id
HAVING COUNT(*) > 1
   OR flight_id IS NULL;


-- ============================================================
-- Inspect one known duplicate flight ID manually
-- Used to understand why duplicates exist
-- ============================================================
SELECT *
FROM bronze.ops_flights
WHERE flight_id = 'SG-2202';


-- ============================================================
-- Review all airline names present in the dataset
-- Helps identify spelling issues or inconsistent airline naming
-- ============================================================
SELECT DISTINCT airline_name
FROM bronze.ops_flights;


-- ============================================================
-- Check airline names for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT airline_name
FROM bronze.ops_flights
WHERE airline_name != TRIM(airline_name);


-- ============================================================
-- Review airline name and airline code mapping
-- Helps confirm that one airline consistently uses one code
-- ============================================================
SELECT DISTINCT airline_name, airline_code
FROM bronze.ops_flights;


-- ============================================================
-- Compare non-null counts for airline name and airline code
-- Helps identify missing airline code values
-- ============================================================
SELECT 
    COUNT(airline_name) AS airline_name_count,
    COUNT(airline_code) AS airline_code_count
FROM bronze.ops_flights;


-- ============================================================
-- Check airline codes for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT airline_code
FROM bronze.ops_flights
WHERE airline_code != TRIM(airline_code);


-- ============================================================
-- Check origin airport values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT origin_airport
FROM bronze.ops_flights
WHERE origin_airport != TRIM(origin_airport);


-- ============================================================
-- Review all destination airport codes in the dataset
-- Useful for understanding airport coverage
-- ============================================================
SELECT DISTINCT destination_airport
FROM bronze.ops_flights;


-- ============================================================
-- Check destination airport values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT destination_airport
FROM bronze.ops_flights
WHERE destination_airport != TRIM(destination_airport);


-- ============================================================
-- Identify records where scheduled departure time is later
-- than actual departure time
-- These flights departed earlier than scheduled
-- ============================================================
SELECT 
    scheduled_departure_time,
    actual_departure_time
FROM bronze.ops_flights
WHERE scheduled_departure_time > actual_departure_time;


-- ============================================================
-- Compare actual departure delay with recorded delay minutes
-- Used to validate departure_delay_minutes column
-- ============================================================
SELECT 
    STR_TO_DATE(scheduled_departure_time, '%Y-%m-%d %H:%i:%s') AS scheduled_departure_time,
    STR_TO_DATE(actual_departure_time, '%Y-%m-%d %H:%i:%s') AS actual_departure_time,
    TIMESTAMPDIFF(
        MINUTE,
        STR_TO_DATE(scheduled_departure_time, '%Y-%m-%d %H:%i:%s'),
        STR_TO_DATE(actual_departure_time, '%Y-%m-%d %H:%i:%s')
    ) AS calculated_departure_delay_minutes,
    departure_delay_minutes
FROM bronze.ops_flights
WHERE scheduled_departure_time < actual_departure_time;


-- ============================================================
-- Flights that arrived earlier than scheduled
-- ============================================================
SELECT 
    scheduled_arrival_time,
    actual_arrival_time
FROM bronze.ops_flights
WHERE scheduled_arrival_time > actual_arrival_time;


-- ============================================================
-- Flights that arrived later than scheduled
-- ============================================================
SELECT 
    scheduled_arrival_time,
    actual_arrival_time
FROM bronze.ops_flights
WHERE scheduled_arrival_time < actual_arrival_time;


-- ============================================================
-- Flights that arrived exactly on time
-- ============================================================
SELECT 
    scheduled_arrival_time,
    actual_arrival_time
FROM bronze.ops_flights
WHERE scheduled_arrival_time = actual_arrival_time;


-- ============================================================
-- Review all aircraft types available in the dataset
-- ============================================================
SELECT DISTINCT aircraft_type
FROM bronze.ops_flights;


-- ============================================================
-- Check aircraft type values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT aircraft_type
FROM bronze.ops_flights
WHERE aircraft_type != TRIM(aircraft_type);


-- ============================================================
-- Review all aircraft registrations
-- ============================================================
SELECT DISTINCT aircraft_registration
FROM bronze.ops_flights;


-- ============================================================
-- Check aircraft registration values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT aircraft_registration
FROM bronze.ops_flights
WHERE aircraft_registration != TRIM(aircraft_registration);


-- ============================================================
-- Check for records where booked passengers exceed seat capacity
-- These rows require transformation before silver load
-- ============================================================
SELECT COUNT(*) AS invalid_capacity_records
FROM bronze.ops_flights
WHERE seat_capacity < booked_passengers;


-- ============================================================
-- Transformation candidate for silver layer:
-- If booked passengers exceed seat capacity,
-- limit booked passengers to seat capacity
-- ============================================================
SELECT 
    booked_passengers,
    seat_capacity,
    LEAST(
        CAST(booked_passengers AS SIGNED),
        CAST(seat_capacity AS SIGNED)
    ) AS adjusted_booked_passengers
FROM bronze.ops_flights
WHERE seat_capacity < booked_passengers;


-- ============================================================
-- Transformation candidate for silver layer:
-- Expand abbreviated delay reasons into full descriptions
-- ============================================================
SELECT 
    delay_reason,
    CASE
        WHEN UPPER(TRIM(delay_reason)) = 'ATC' THEN 'AIR TRAFFIC CONTROL'
        WHEN UPPER(TRIM(delay_reason)) = 'WX' THEN 'WEATHER'
        WHEN UPPER(TRIM(delay_reason)) = 'TECH' THEN 'TECHNICAL'
        ELSE delay_reason
    END AS delay_reason_standardized
FROM bronze.ops_flights;


-- ============================================================
-- Validate that boarding starts before actual departure
-- Expected result: 0 records
-- ============================================================
SELECT 
    boarding_start_time,
    actual_departure_time
FROM bronze.ops_flights
WHERE boarding_start_time > actual_departure_time;


-- ============================================================
-- Review delay category against delay minutes
-- Helps determine whether delay_category is consistent
-- ============================================================
SELECT 
    departure_delay_minutes,
    delay_category
FROM bronze.ops_flights
WHERE departure_delay_minutes > 0;


-- ============================================================
-- Transformation candidate for silver layer:
-- Recalculate delay category from delay minutes
-- ============================================================
SELECT 
    delay_category,
    CASE
        WHEN CAST(departure_delay_minutes AS SIGNED) = 0 THEN 'On-Time'
        WHEN CAST(departure_delay_minutes AS SIGNED) BETWEEN 1 AND 60 THEN 'Moderate'
        WHEN CAST(departure_delay_minutes AS SIGNED) BETWEEN 61 AND 120 THEN 'Moderate-High'
        ELSE 'High'
    END AS recalculated_delay_category
FROM bronze.ops_flights;


-- ============================================================
-- Validate departure day of week against actual departure timestamp
-- Helps identify whether departure_day_of_week is based on
-- scheduled departure or actual departure
-- ============================================================
SELECT
    departure_delay_minutes,
    scheduled_departure_time,
    actual_departure_time,
    departure_day_of_week,
    DATE_FORMAT(
        STR_TO_DATE(scheduled_departure_time, '%Y-%m-%d %H:%i:%s'),
        '%a'
    ) AS scheduled_departure_day,
    DATE_FORMAT(
        STR_TO_DATE(actual_departure_time, '%Y-%m-%d %H:%i:%s'),
        '%a'
    ) AS actual_departure_day
FROM bronze.ops_flights
WHERE departure_day_of_week != DATE_FORMAT(
    STR_TO_DATE(actual_departure_time, '%Y-%m-%d %H:%i:%s'),
    '%a'
);


-- ============================================================
-- Validate weekend flight flag against actual departure day
-- Helps determine whether weekend flag is derived correctly
-- ============================================================
SELECT
    departure_delay_minutes,
    scheduled_departure_time,
    actual_departure_time,
    departure_day_of_week,
    DATE_FORMAT(
        STR_TO_DATE(scheduled_departure_time, '%Y-%m-%d %H:%i:%s'),
        '%a'
    ) AS scheduled_departure_day,
    weekend_flight_flag,
    DATE_FORMAT(
        STR_TO_DATE(actual_departure_time, '%Y-%m-%d %H:%i:%s'),
        '%a'
    ) AS actual_departure_day
FROM bronze.ops_flights
WHERE departure_day_of_week != DATE_FORMAT(
    STR_TO_DATE(actual_departure_time, '%Y-%m-%d %H:%i:%s'),
    '%a'
);


-- ============================================================
-- Validate route type against international flight flag
-- Helps confirm consistency between international indicator
-- and route classification
-- ============================================================
SELECT DISTINCT 
    international_flight_flag,
    route_type
FROM bronze.ops_flights
WHERE international_flight_flag = 'TRUE';