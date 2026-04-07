/*
===============================================================================
Gold Layer Views - MySQL Version
===============================================================================
Notes:
- MySQL does not support OBJECT_ID or GO
- Use DROP VIEW IF EXISTS
- Window functions are supported in MySQL 8+
===============================================================================
*/

-- ============================================================================
-- 1. gold.dim_flights
-- ============================================================================

DROP VIEW IF EXISTS gold.dim_flights;

CREATE VIEW gold.dim_flights AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY f.flight_id
    ) AS flight_key,

    f.flight_id,
    f.airline_name,
    f.airline_code,
    f.origin_airport,
    f.destination_airport,
    CONCAT(f.origin_airport, ' - ', f.destination_airport) AS route_name,
    f.route_type,
    f.aircraft_type,
    f.aircraft_registration,
    f.terminal,
    f.gate_number,
    f.season,
    f.departure_day_of_week,
    f.arrival_time_of_day,
    f.international_flight_flag,
    f.weekend_flight_flag,
    f.flight_status,
    f.delay_category,
    f.departure_delay_minutes,
    f.flight_duration_minutes,
    f.seat_capacity,
    f.booked_passengers,
    ROUND(
        (f.booked_passengers / NULLIF(f.seat_capacity, 0)) * 100,
        2
    ) AS recalculated_load_factor_percentage,
    f.ticket_revenue
FROM silver.ops_flights f;


-- ============================================================================
-- 2. gold.dim_passengers
-- ============================================================================

DROP VIEW IF EXISTS gold.dim_passengers;

CREATE VIEW gold.dim_passengers AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY p.passenger_id
    ) AS passenger_key,

    p.passenger_id,
    p.loyalty_number,
    CONCAT(p.first_name, ' ', p.last_name) AS passenger_name,
    p.nationality,
    p.gender,
    p.date_of_birth,
    p.passenger_age,
    p.age_group,
    p.travel_class,
    p.booking_channel,
    p.vip_flag,
    p.no_show_flag,
    p.satisfaction_score,

    CASE
        WHEN p.vip_flag = 1 THEN 'VIP Passenger'
        WHEN p.travel_class = 'First' THEN 'Premium Passenger'
        WHEN p.travel_class = 'Business' THEN 'Business Passenger'
        ELSE 'Regular Passenger'
    END AS passenger_segment

FROM silver.pax_passengers p;


-- ============================================================================
-- 3. gold.fact_flight_operations
-- ============================================================================

DROP VIEW IF EXISTS gold.fact_flight_operations;

CREATE VIEW gold.fact_flight_operations AS
SELECT
    df.flight_key,
    f.flight_id,
    f.airline_name,
    f.origin_airport,
    f.destination_airport,
    f.route_type,
    f.terminal,
    f.gate_number,
    f.scheduled_departure_time,
    f.actual_departure_time,
    f.scheduled_arrival_time,
    f.actual_arrival_time,
    f.departure_delay_minutes,

    TIMESTAMPDIFF(
        MINUTE,
        f.scheduled_arrival_time,
        f.actual_arrival_time
    ) AS arrival_delay_minutes,

    f.delay_category,
    f.flight_status,
    f.booked_passengers,
    f.seat_capacity,
    f.load_factor_percentage,
    f.flight_distance_km,
    f.flight_duration_minutes,
    f.ticket_revenue,
    f.baggage_load_tons,

    COUNT(DISTINCT p.passenger_id) AS passenger_count,
    COUNT(DISTINCT b.baggage_tag_number) AS baggage_count,

    ROUND(AVG(b.baggage_weight_kg), 2) AS avg_baggage_weight_kg,

    SUM(
        CASE
            WHEN b.baggage_status IN ('Lost', 'Damaged')
            THEN 1
            ELSE 0
        END
    ) AS baggage_issue_count,

    DENSE_RANK() OVER (
        PARTITION BY f.airline_name
        ORDER BY f.ticket_revenue DESC
    ) AS airline_revenue_rank,

    DENSE_RANK() OVER (
        ORDER BY f.departure_delay_minutes DESC
    ) AS overall_delay_rank

FROM silver.ops_flights f
LEFT JOIN gold.dim_flights df
    ON f.flight_id = df.flight_id
LEFT JOIN silver.pax_passengers p
    ON f.flight_id = p.flight_number
LEFT JOIN silver.ops_baggage b
    ON f.flight_id = b.flight_id
GROUP BY
    df.flight_key,
    f.flight_id,
    f.airline_name,
    f.origin_airport,
    f.destination_airport,
    f.route_type,
    f.terminal,
    f.gate_number,
    f.scheduled_departure_time,
    f.actual_departure_time,
    f.scheduled_arrival_time,
    f.actual_arrival_time,
    f.departure_delay_minutes,
    f.delay_category,
    f.flight_status,
    f.booked_passengers,
    f.seat_capacity,
    f.load_factor_percentage,
    f.flight_distance_km,
    f.flight_duration_minutes,
    f.ticket_revenue,
    f.baggage_load_tons;


-- ============================================================================
-- 4. gold.fact_passenger_journey
-- ============================================================================

DROP VIEW IF EXISTS gold.fact_passenger_journey;

CREATE VIEW gold.fact_passenger_journey AS
SELECT
    dp.passenger_key,
    df.flight_key,
    p.passenger_id,
    CONCAT(p.first_name, ' ', p.last_name) AS passenger_name,
    p.flight_number,
    p.travel_class,
    p.age_group,
    p.nationality,
    p.booking_channel,
    p.vip_flag,
    p.no_show_flag,
    p.baggage_count,

    TIMESTAMPDIFF(
        MINUTE,
        p.checkin_time,
        p.boarding_time
    ) AS checkin_to_boarding_minutes,

    ge.event_type,
    ge.gate_number,
    ge.terminal,
    ge.passenger_count AS gate_passenger_count,

    CASE
        WHEN p.no_show_flag = 1 THEN 'No Show'
        WHEN p.boarding_time IS NOT NULL THEN 'Boarded'
        ELSE 'Not Boarded'
    END AS boarding_status,

    ROW_NUMBER() OVER (
        PARTITION BY p.passenger_id
        ORDER BY p.boarding_time DESC
    ) AS passenger_trip_number,

    COUNT(*) OVER (
        PARTITION BY p.passenger_id
    ) AS total_flights_taken,

    ROUND(
        AVG(p.satisfaction_score) OVER (
            PARTITION BY p.nationality
        ),
        2
    ) AS avg_nationality_satisfaction_score

FROM silver.pax_passengers p
LEFT JOIN gold.dim_passengers dp
    ON p.passenger_id = dp.passenger_id
LEFT JOIN gold.dim_flights df
    ON p.flight_number = df.flight_id
LEFT JOIN silver.ops_gate_events ge
    ON p.flight_number = ge.flight_id
   AND p.gate_number = ge.gate_number;


-- ============================================================================
-- 5. gold.fact_baggage_performance
-- ============================================================================

DROP VIEW IF EXISTS gold.fact_baggage_performance;

CREATE VIEW gold.fact_baggage_performance AS
SELECT
    df.flight_key,
    dp.passenger_key,
    b.baggage_tag_number,
    b.flight_id,
    b.passenger_id,
    b.baggage_type,
    b.baggage_weight_kg,
    b.baggage_status,
    b.baggage_delay_minutes,
    b.baggage_location,
    b.oversized_baggage_flag,
    b.damaged_baggage_flag,
    b.terminal_number,

    TIMESTAMPDIFF(
        MINUTE,
        b.checkin_time,
        b.baggage_scan_time
    ) AS checkin_to_scan_minutes,

    TIMESTAMPDIFF(
        MINUTE,
        b.baggage_scan_time,
        b.baggage_loaded_time
    ) AS scan_to_load_minutes,

    CASE
        WHEN b.baggage_status = 'Lost' THEN 'Critical'
        WHEN b.damaged_baggage_flag = 1 THEN 'Damaged'
        WHEN b.baggage_delay_minutes > 30 THEN 'Delayed'
        ELSE 'Normal'
    END AS baggage_issue_category,

    ROW_NUMBER() OVER (
        PARTITION BY b.flight_id
        ORDER BY b.baggage_weight_kg DESC
    ) AS baggage_weight_rank_in_flight,

    ROUND(
        AVG(b.baggage_weight_kg) OVER (
            PARTITION BY b.flight_id
        ),
        2
    ) AS avg_baggage_weight_per_flight

FROM silver.ops_baggage b
LEFT JOIN gold.dim_flights df
    ON b.flight_id = df.flight_id
LEFT JOIN gold.dim_passengers dp
    ON b.passenger_id = dp.passenger_id;


-- ============================================================================
-- 6. gold.fact_retail_sales
-- ============================================================================

DROP VIEW IF EXISTS gold.fact_retail_sales;

CREATE VIEW gold.fact_retail_sales AS
SELECT
    dp.passenger_key,
    df.flight_key,
    r.transaction_id,
    r.transaction_timestamp,
    r.flight_number,
    r.store_name,
    r.store_category,
    r.product_category,
    r.payment_method,
    r.currency,
    r.terminal,
    r.store_location,
    r.quantity,
    r.unit_price,
    r.total_amount,
    r.loyalty_points_earned,
    r.duty_free_flag,

    p.nationality,
    p.travel_class,
    p.age_group,
    p.vip_flag,

    f.airline_name,
    f.route_type,
    f.destination_airport,

    SUM(r.total_amount) OVER (
        PARTITION BY r.flight_number
    ) AS total_retail_sales_per_flight,

    SUM(r.total_amount) OVER (
        PARTITION BY r.terminal
    ) AS total_retail_sales_per_terminal,

    ROUND(
        AVG(r.total_amount) OVER (
            PARTITION BY p.nationality
        ),
        2
    ) AS avg_spend_by_nationality,

    DENSE_RANK() OVER (
        PARTITION BY r.terminal
        ORDER BY r.total_amount DESC
    ) AS spend_rank_within_terminal,

    ROW_NUMBER() OVER (
        PARTITION BY r.passenger_passport_number
        ORDER BY r.transaction_timestamp
    ) AS passenger_purchase_sequence

FROM silver.ret_retail_transactions r
LEFT JOIN silver.pax_passengers p
    ON r.passenger_passport_number = p.passenger_passport_number
LEFT JOIN silver.ops_flights f
    ON r.flight_number = f.flight_id
LEFT JOIN gold.dim_passengers dp
    ON p.passenger_id = dp.passenger_id
LEFT JOIN gold.dim_flights df
    ON f.flight_id = df.flight_id;