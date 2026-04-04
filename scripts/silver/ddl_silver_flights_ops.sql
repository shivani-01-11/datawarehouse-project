USE aviation_dw;

-- ============================================================
-- 1. silver.ops_flights
--    Source  : bronze.ops_flights
--    Grain   : one row per flight leg
--    Changes :
--      - Added surrogate primary key column: id
--      - Converted airport abbreviations to city names
--      - Converted delay reason abbreviations to full forms
--      - Recalculated delay category from delay minutes
--      - Converted booleans to TINYINT(1)
--      - Cast datetime and numeric columns
--      - Capped booked passengers at seat capacity
--      - Capped load factor at 100.00
-- ============================================================
DROP TABLE IF EXISTS silver.ops_flights;

CREATE TABLE silver.ops_flights (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    flight_id                   VARCHAR(20),
    airline_name                VARCHAR(100),
    airline_code                VARCHAR(10),
    origin_airport              VARCHAR(50),
    destination_airport         VARCHAR(50),
    scheduled_departure_time    DATETIME,
    actual_departure_time       DATETIME,
    scheduled_arrival_time      DATETIME,
    actual_arrival_time         DATETIME,
    aircraft_type               VARCHAR(20),
    aircraft_registration       VARCHAR(20),
    seat_capacity               INT,
    booked_passengers           INT,
    flight_status               VARCHAR(30),
    departure_delay_minutes     INT,
    delay_reason                VARCHAR(100),
    terminal                    VARCHAR(10),
    gate_number                 VARCHAR(10),
    international_flight_flag   TINYINT(1),
    flight_distance_km          INT,
    ticket_revenue              DECIMAL(12,2),
    boarding_start_time         DATETIME,
    delayed_flight_flag         TINYINT(1),
    delay_category              VARCHAR(30),
    load_factor_percentage      DECIMAL(5,2),
    flight_duration_minutes     INT,
    baggage_load_tons           DECIMAL(8,4),
    arrival_time_of_day         VARCHAR(20),
    departure_day_of_week       VARCHAR(10),
    weekend_flight_flag         TINYINT(1),
    season                      VARCHAR(20),
    route_type                  VARCHAR(30),
    dwh_create_date             DATETIME DEFAULT NOW()
);


-- ============================================================
-- Load silver.ops_flights from bronze.ops_flights
-- ============================================================

-- SET v_start_time = NOW();

SELECT '>> Truncating Table: silver.ops_flights' AS load_log;
TRUNCATE TABLE silver.ops_flights;

SELECT '>> Inserting Data Into: silver.ops_flights' AS load_log;

INSERT INTO silver.ops_flights (
    flight_id,
    airline_name,
    airline_code,
    origin_airport,
    destination_airport,
    scheduled_departure_time,
    actual_departure_time,
    scheduled_arrival_time,
    actual_arrival_time,
    aircraft_type,
    aircraft_registration,
    seat_capacity,
    booked_passengers,
    flight_status,
    departure_delay_minutes,
    delay_reason,
    terminal,
    gate_number,
    international_flight_flag,
    flight_distance_km,
    ticket_revenue,
    boarding_start_time,
    delayed_flight_flag,
    delay_category,
    load_factor_percentage,
    flight_duration_minutes,
    baggage_load_tons,
    arrival_time_of_day,
    departure_day_of_week,
    weekend_flight_flag,
    season,
    route_type
)
SELECT
    TRIM(flight_id),
    TRIM(airline_name),
    UPPER(TRIM(airline_code)),

    -- Convert origin airport code to city name
    CASE
        WHEN UPPER(TRIM(origin_airport)) = 'DEL' THEN 'Delhi'
        ELSE TRIM(origin_airport)
    END AS origin_airport,

    -- Convert destination airport code to city name
    CASE
        WHEN UPPER(TRIM(destination_airport)) = 'SIN' THEN 'Singapore'
        WHEN UPPER(TRIM(destination_airport)) = 'DXB' THEN 'Dubai'
        WHEN UPPER(TRIM(destination_airport)) = 'MAA' THEN 'Chennai'
        WHEN UPPER(TRIM(destination_airport)) = 'KUL' THEN 'Kuala Lumpur'
        WHEN UPPER(TRIM(destination_airport)) = 'AMS' THEN 'Amsterdam'
        WHEN UPPER(TRIM(destination_airport)) = 'BLR' THEN 'Bengaluru'
        WHEN UPPER(TRIM(destination_airport)) = 'CDG' THEN 'Paris'
        WHEN UPPER(TRIM(destination_airport)) = 'BOM' THEN 'Mumbai'
        WHEN UPPER(TRIM(destination_airport)) = 'HYD' THEN 'Hyderabad'
        WHEN UPPER(TRIM(destination_airport)) = 'FRA' THEN 'Frankfurt'
        WHEN UPPER(TRIM(destination_airport)) = 'JFK' THEN 'New York'
        WHEN UPPER(TRIM(destination_airport)) = 'CCU' THEN 'Kolkata'
        WHEN UPPER(TRIM(destination_airport)) = 'LHR' THEN 'London'
        WHEN UPPER(TRIM(destination_airport)) = 'DOH' THEN 'Doha'
        ELSE TRIM(destination_airport)
    END AS destination_airport,

    STR_TO_DATE(scheduled_departure_time, '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(actual_departure_time, '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(scheduled_arrival_time, '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(actual_arrival_time, '%Y-%m-%d %H:%i:%s'),

    TRIM(aircraft_type),
    UPPER(TRIM(aircraft_registration)),

    CAST(seat_capacity AS SIGNED),

    -- Ensure booked passengers do not exceed seat capacity
    LEAST(
        CAST(booked_passengers AS SIGNED),
        CAST(seat_capacity AS SIGNED)
    ) AS booked_passengers,

    TRIM(flight_status),

    CAST(departure_delay_minutes AS SIGNED),

    -- Convert abbreviated delay reasons into full descriptions
    CASE
        WHEN UPPER(TRIM(delay_reason)) = 'ATC' THEN 'AIR TRAFFIC CONTROL'
        WHEN UPPER(TRIM(delay_reason)) = 'WX' THEN 'WEATHER'
        WHEN UPPER(TRIM(delay_reason)) = 'TECH' THEN 'TECHNICAL'
        WHEN UPPER(TRIM(delay_reason)) = 'CREW' THEN 'CREW ISSUE'
        WHEN UPPER(TRIM(delay_reason)) = 'TURNAROUND' THEN 'AIRCRAFT TURNAROUND'
        ELSE UPPER(TRIM(delay_reason))
    END AS delay_reason,

    UPPER(TRIM(terminal)),
    UPPER(TRIM(gate_number)),

    CASE
        WHEN UPPER(TRIM(international_flight_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS international_flight_flag,

    CAST(flight_distance_km AS SIGNED),

    ROUND(CAST(ticket_revenue AS DECIMAL(12,2)), 2),

    STR_TO_DATE(boarding_start_time, '%Y-%m-%d %H:%i:%s'),

    CASE
        WHEN UPPER(TRIM(delayed_flight_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS delayed_flight_flag,

    -- Recalculate delay category using departure delay minutes
    CASE
        WHEN CAST(departure_delay_minutes AS SIGNED) = 0 THEN 'On-Time'
        WHEN CAST(departure_delay_minutes AS SIGNED) BETWEEN 1 AND 60 THEN 'Moderate'
        WHEN CAST(departure_delay_minutes AS SIGNED) BETWEEN 61 AND 120 THEN 'Moderate-High'
        ELSE 'High'
    END AS delay_category,

    LEAST(
        ROUND(CAST(load_factor_percentage AS DECIMAL(5,2)), 2),
        100.00
    ) AS load_factor_percentage,

    CAST(flight_duration_minutes AS SIGNED),

    ROUND(CAST(baggage_load_tons AS DECIMAL(8,4)), 4),

    TRIM(arrival_time_of_day),
    TRIM(departure_day_of_week),

    CASE
        WHEN UPPER(TRIM(weekend_flight_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS weekend_flight_flag,

    TRIM(season),
    TRIM(route_type)

FROM bronze.ops_flights
WHERE NULLIF(TRIM(flight_id), '') IS NOT NULL;


select * from silver.ops_flights;