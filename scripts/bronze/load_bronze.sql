-- ============================================================
-- This script loads all raw source CSV files into the Bronze layer
-- of the Aviation Data Warehouse using MySQL LOAD DATA commands.
-- It first truncates existing Bronze tables to support full reloads,
-- then imports flights, passengers, baggage, gate events,
-- and retail transaction datasets from source files.
-- The script also adds technical metadata such as source system,
-- load timestamp, batch ID, and source file name for tracking.
-- Finally, it validates the load by checking row counts in each table.
-- ============================================================

USE aviation_dw;

SET GLOBAL local_infile = 1;


DELIMITER $$

DROP PROCEDURE IF EXISTS bronze.truncate_bronze_tables$$

CREATE PROCEDURE bronze.truncate_bronze_tables()
BEGIN
    TRUNCATE TABLE bronze.ops_flights;
    TRUNCATE TABLE bronze.pax_passengers;
    TRUNCATE TABLE bronze.ops_baggage;
    TRUNCATE TABLE bronze.ops_gate_events;
    TRUNCATE TABLE bronze.sec_security_screening;
    TRUNCATE TABLE bronze.wf_staff_shifts;
    TRUNCATE TABLE bronze.ret_retail_transactions;
    TRUNCATE TABLE bronze.mnt_maintenance_logs;

    SELECT 'All bronze tables truncated successfully' AS status_message;
END$$

DELIMITER ;

CALL bronze.truncate_bronze_tables();

-- ============================================================
-- Load bronze.ops_flights
-- ============================================================

TRUNCATE TABLE bronze.ops_flights;

LOAD DATA LOCAL INFILE '/airport-operations-dataset/datasets/flights.csv'
INTO TABLE bronze.ops_flights
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    flight_id, airline_name, airline_code, origin_airport, destination_airport,
    scheduled_departure_time, actual_departure_time,
    scheduled_arrival_time, actual_arrival_time,
    aircraft_type, aircraft_registration, seat_capacity, booked_passengers,
    flight_status, departure_delay_minutes, delay_reason,
    terminal, gate_number, international_flight_flag,
    flight_distance_km, ticket_revenue, boarding_start_time,
    delayed_flight_flag, delay_category, load_factor_percentage,
    flight_duration_minutes, baggage_load_tons,
    arrival_time_of_day, departure_day_of_week, weekend_flight_flag,
    season, route_type
)
SET
    dwh_source_system = 'AOMS',
    dwh_load_date     = NOW(),
    dwh_batch_id      = CONCAT('batch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s')),
    dwh_file_name     = 'flights.csv';



-- ============================================================
-- Load bronze.pax_passengers
-- ============================================================

TRUNCATE TABLE bronze.pax_passengers;

LOAD DATA LOCAL INFILE '/airport-operations-dataset/datasets/passengers.csv'
INTO TABLE bronze.pax_passengers
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    passenger_id, loyalty_number, passenger_passport_number,
    first_name, last_name, nationality, date_of_birth, gender,
    seat_number, travel_class, flight_number,
    checkin_time, boarding_time, gate_number, baggage_count,
    special_meal_request, wheelchair_assistance_flag, unaccompanied_minor_flag,
    email_address, phone_number, frequent_flyer_tier, passport_expiry_date,
    no_show_flag, satisfaction_score, vip_flag, booking_channel,
    passenger_age, age_group
)
SET
    dwh_source_system = 'PAX',
    dwh_load_date     = NOW(),
    dwh_batch_id      = CONCAT('batch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s')),
    dwh_file_name     = 'passengers.csv';



-- ============================================================
-- Load bronze.ops_baggage
-- ============================================================

TRUNCATE TABLE bronze.ops_baggage;


LOAD DATA LOCAL INFILE '/airport-operations-dataset/datasets/baggage3.csv'
INTO TABLE bronze.ops_baggage
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    baggage_tag_number, passenger_id, flight_id, passenger_passport_number,
    baggage_weight_kg, baggage_dimensions_cm, baggage_type,
    checkin_counter, checkin_time, baggage_scan_time,
    terminal_number, baggage_status, oversized_baggage_flag,
    baggage_delay_minutes, baggage_location, baggage_loaded_time,
    damaged_baggage_flag,
    @dummy
)
SET
    dwh_source_system = 'AOMS',
    dwh_load_date     = NOW(),
    dwh_batch_id      = CONCAT('batch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s')),
    dwh_file_name     = 'baggage.csv';



-- ============================================================
-- Load bronze.ops_gate_events
-- ============================================================

TRUNCATE TABLE bronze.ops_gate_events;

LOAD DATA LOCAL INFILE '/airport-operations-dataset/datasets/gate_events.csv'
INTO TABLE bronze.ops_gate_events
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    gate_event_id, flight_id, gate_number, terminal,
    event_type, event_timestamp, staff_id, passenger_count,
    event_category, gate_change_flag, previous_event_timestamp,
    gate_open_time, gate_close_time, boarding_completion_time
)
SET
    dwh_source_system = 'AOMS',
    dwh_load_date     = NOW(),
    dwh_batch_id      = CONCAT('batch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s')),
    dwh_file_name     = 'gate_events.csv';



-- ============================================================
-- Load bronze.ret_retail_transactions
-- ============================================================

TRUNCATE TABLE bronze.ret_retail_transactions;

LOAD DATA LOCAL INFILE '/airport-operations-dataset/datasets/retail_transactions.csv'
INTO TABLE bronze.ret_retail_transactions
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    transaction_id, staff_id, store_name, store_category,
    passenger_passport_number, flight_number, transaction_timestamp,
    product_category, quantity, unit_price, total_amount,
    payment_method, currency, loyalty_points_earned,
    terminal, store_location, duty_free_flag
)
SET
    dwh_source_system = 'POS',
    dwh_load_date     = NOW(),
    dwh_batch_id      = CONCAT('batch_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s')),
    dwh_file_name     = 'retail_transactions.csv';



DELIMITER $$

DROP PROCEDURE IF EXISTS bronze.validate_bronze_load$$

CREATE PROCEDURE bronze.validate_bronze_load()
BEGIN
    SELECT 'bronze.ops_flights' AS table_name, COUNT(*) AS row_count
    FROM bronze.ops_flights

    UNION ALL

    SELECT 'bronze.pax_passengers', COUNT(*)
    FROM bronze.pax_passengers

    UNION ALL

    SELECT 'bronze.ops_baggage', COUNT(*)
    FROM bronze.ops_baggage
    
    UNION ALL

    SELECT 'bronze.ops_baggage', COUNT(*)
    FROM bronze.ops_gate_events
    
    UNION ALL

    SELECT 'bronze.ops_baggage', COUNT(*)
    FROM bronze.ret_retail_transactions

END$$

DELIMITER ;

CALL bronze.validate_bronze_load();
