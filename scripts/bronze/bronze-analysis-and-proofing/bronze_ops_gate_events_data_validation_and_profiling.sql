-- ============================================================
-- File Name : bronze_ops_gate_events_data_validation_and_profiling.sql
-- Purpose   : Validate and profile bronze.ops_gate_events
--             before loading into silver layer
-- ============================================================


-- ============================================================
-- Check total number of records in gate events table
-- ============================================================
SELECT COUNT(*) AS total_record_count
FROM bronze.ops_gate_events;


-- ============================================================
-- Check for duplicate or null gate_event_id values
-- gate_event_id should ideally uniquely identify each event
-- ============================================================
SELECT
    gate_event_id,
    COUNT(*) AS duplicate_count
FROM bronze.ops_gate_events
GROUP BY gate_event_id
HAVING COUNT(*) > 1
   OR gate_event_id IS NULL;


-- ============================================================
-- Check gate_event_id values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT gate_event_id
FROM bronze.ops_gate_events
WHERE gate_event_id != TRIM(gate_event_id);


-- ============================================================
-- Check flight_id values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT flight_id
FROM bronze.ops_gate_events
WHERE flight_id != TRIM(flight_id);


-- ============================================================
-- Check whether all flight_ids exist in ops_flights
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT flight_id
FROM bronze.ops_gate_events
WHERE TRIM(flight_id) NOT IN (
    SELECT DISTINCT TRIM(flight_id)
    FROM silver.ops_flights
);


-- ============================================================
-- Check gate_number values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT gate_number
FROM bronze.ops_gate_events
WHERE gate_number != TRIM(gate_number);


-- ============================================================
-- Review distinct gate numbers
-- ============================================================
SELECT DISTINCT gate_number
FROM bronze.ops_gate_events;


-- ============================================================
-- Review terminal values
-- Source currently appears to contain only T3
-- ============================================================
SELECT DISTINCT terminal
FROM bronze.ops_gate_events;


-- ============================================================
-- Check whether gate_event_id prefix matches terminal column
-- Example: T3-R18-474592 should correspond to terminal = T3
-- Expected result: 0 records
-- ============================================================
SELECT
    gate_event_id,
    terminal
FROM bronze.ops_gate_events
WHERE SUBSTRING_INDEX(TRIM(gate_event_id), '-', 1) != TRIM(terminal);


-- ============================================================
-- Review event_type values
-- Source appears to contain only boarding events
-- ============================================================
SELECT DISTINCT event_type
FROM bronze.ops_gate_events;


-- ============================================================
-- Check event_type values for leading or trailing spaces
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT event_type
FROM bronze.ops_gate_events
WHERE event_type != TRIM(event_type);


-- ============================================================
-- Review passenger_count values
-- Source appears to contain only 120
-- ============================================================
SELECT DISTINCT passenger_count
FROM bronze.ops_gate_events;


-- ============================================================
-- Check for invalid passenger_count values
-- passenger_count should not be null, blank or negative
-- ============================================================
SELECT
    passenger_count
FROM bronze.ops_gate_events
WHERE passenger_count IS NULL
   OR TRIM(passenger_count) = ''
   OR CAST(passenger_count AS SIGNED) < 0;


-- ============================================================
-- Review event_category values
-- Source appears to contain only Routine
-- ============================================================
SELECT DISTINCT event_category
FROM bronze.ops_gate_events;


-- ============================================================
-- Review gate_change_flag values
-- Source appears to contain only FALSE
-- ============================================================
SELECT DISTINCT gate_change_flag
FROM bronze.ops_gate_events;


-- ============================================================
-- Check whether previous_event_timestamp column is empty
-- If all values are blank, this column can be dropped in silver
-- ============================================================
SELECT
    COUNT(*) AS blank_previous_event_timestamp_count
FROM bronze.ops_gate_events
WHERE TRIM(COALESCE(previous_event_timestamp, '')) = '';


-- ============================================================
-- Check for leading or trailing spaces in staff_id
-- Expected result: 0 records
-- ============================================================
SELECT DISTINCT staff_id
FROM bronze.ops_gate_events
WHERE staff_id != TRIM(staff_id);


-- ============================================================
-- Check whether all staff_ids exist in wf_staff_shifts
-- Expected result: 0 records
-- ============================================================
-- SELECT DISTINCT staff_id
-- FROM bronze.ops_gate_events
-- WHERE TRIM(staff_id) NOT IN (
--     SELECT DISTINCT TRIM(staff_id)
--     FROM bronze.wf_staff_shifts
-- );


-- ============================================================
-- Review gate_open_time values compared to gate_close_time
-- Gate should normally open before it closes
-- Expected result: 0 records
-- ============================================================
SELECT
    gate_event_id,
    gate_open_time,
    gate_close_time
FROM bronze.ops_gate_events
WHERE STR_TO_DATE(gate_open_time, '%Y-%m-%d %H:%i:%s')
      > STR_TO_DATE(gate_close_time, '%Y-%m-%d %H:%i:%s');


-- ============================================================
-- Review gate_close_time compared to boarding_completion_time
-- Boarding completion should normally happen before or at gate close
-- Expected result: 0 records
-- ============================================================
SELECT
    gate_event_id,
    gate_close_time,
    boarding_completion_time
FROM bronze.ops_gate_events
WHERE STR_TO_DATE(boarding_completion_time, '%Y-%m-%d %H:%i:%s')
      > STR_TO_DATE(gate_close_time, '%Y-%m-%d %H:%i:%s');


-- ============================================================
-- Review gate_open_time compared to boarding_completion_time
-- Boarding completion should happen after gate open
-- Expected result: 0 records
-- ============================================================
SELECT
    gate_event_id,
    gate_open_time,
    boarding_completion_time
FROM bronze.ops_gate_events
WHERE STR_TO_DATE(gate_open_time, '%Y-%m-%d %H:%i:%s')
      > STR_TO_DATE(boarding_completion_time, '%Y-%m-%d %H:%i:%s');


-- ============================================================
-- Check complete timestamp sequence
-- Expected order:
-- gate_open_time <= boarding_completion_time <= gate_close_time
-- Expected result: 0 records
-- ============================================================
SELECT
    gate_event_id,
    gate_open_time,
    boarding_completion_time,
    gate_close_time
FROM bronze.ops_gate_events
WHERE STR_TO_DATE(gate_open_time, '%Y-%m-%d %H:%i:%s')
          > STR_TO_DATE(boarding_completion_time, '%Y-%m-%d %H:%i:%s')
   OR STR_TO_DATE(boarding_completion_time, '%Y-%m-%d %H:%i:%s')
          > STR_TO_DATE(gate_close_time, '%Y-%m-%d %H:%i:%s');


-- ============================================================
-- Count number of rows with invalid gate event timestamp sequence
-- ============================================================
SELECT
    COUNT(*) AS invalid_gate_time_sequence_count
FROM bronze.ops_gate_events
WHERE STR_TO_DATE(gate_open_time, '%Y-%m-%d %H:%i:%s')
          > STR_TO_DATE(boarding_completion_time, '%Y-%m-%d %H:%i:%s')
   OR STR_TO_DATE(boarding_completion_time, '%Y-%m-%d %H:%i:%s')
          > STR_TO_DATE(gate_close_time, '%Y-%m-%d %H:%i:%s');


-- ============================================================
-- Review event_timestamp values
-- ============================================================
SELECT
    gate_event_id,
    event_timestamp
FROM bronze.ops_gate_events;


-- ============================================================
-- Check whether event_timestamp falls within gate open and close times
-- Expected result: 0 records
-- ============================================================
SELECT
    gate_event_id,
    event_timestamp,
    gate_open_time,
    gate_close_time
FROM bronze.ops_gate_events
WHERE STR_TO_DATE(event_timestamp, '%Y-%m-%d %H:%i:%s')
      < STR_TO_DATE(gate_open_time, '%Y-%m-%d %H:%i:%s')
   OR STR_TO_DATE(event_timestamp, '%Y-%m-%d %H:%i:%s')
      > STR_TO_DATE(gate_close_time, '%Y-%m-%d %H:%i:%s');