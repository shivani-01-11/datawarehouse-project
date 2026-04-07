-- ============================================================
-- silver.ops_gate_events
-- Source  : bronze.ops_gate_events
-- Grain   : one row per gate event
-- Changes :
--   - Added surrogate primary key column: id
--   - passenger_count converted to INT
--   - gate_change_flag converted to TINYINT
--   - previous_event_timestamp converted to NULL
--   - event_timestamp converted to DATETIME
--   - gate timing columns corrected using realistic boarding flow
-- ============================================================

DROP TABLE IF EXISTS silver.ops_gate_events;

CREATE TABLE silver.ops_gate_events (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    gate_event_id               VARCHAR(50),
    flight_id                   VARCHAR(20),
    gate_number                 VARCHAR(10),
    terminal                    VARCHAR(10),
    event_type                  VARCHAR(50),
    event_timestamp             DATETIME,
    staff_id                    VARCHAR(20),
    passenger_count             INT,
    event_category              VARCHAR(30),
    gate_change_flag            TINYINT,
    previous_event_timestamp    DATETIME,
    gate_open_time              DATETIME,
    gate_close_time             DATETIME,
    boarding_completion_time    DATETIME,
    dwh_create_date             DATETIME DEFAULT NOW()
);



-- ============================================================
-- Load silver.ops_gate_events from bronze.ops_gate_events
-- ============================================================

TRUNCATE TABLE silver.ops_gate_events;

SELECT '>> Inserting Data Into: silver.ops_gate_events' AS load_log;

INSERT INTO silver.ops_gate_events (
    gate_event_id,
    flight_id,
    gate_number,
    terminal,
    event_type,
    event_timestamp,
    staff_id,
    passenger_count,
    event_category,
    gate_change_flag,
    previous_event_timestamp,
    gate_open_time,
    gate_close_time,
    boarding_completion_time
)
SELECT
    TRIM(gate_event_id),

    TRIM(flight_id),

    -- Source currently contains only B12
    -- Still standardize formatting for future loads
    UPPER(TRIM(gate_number)),

    -- Keep terminal from source data
    UPPER(TRIM(terminal)),

    -- Source currently contains Boarding Start
    CASE
        WHEN UPPER(TRIM(event_type)) IN ('BOARDING START', 'BOARDING_START')
            THEN 'Boarding Start'
        ELSE TRIM(event_type)
    END AS event_type,

    -- Main event timestamp stored as boarding start time
    STR_TO_DATE(event_timestamp, '%Y-%m-%d %H:%i:%s') AS event_timestamp,

    TRIM(staff_id),

    CAST(passenger_count AS SIGNED),

    -- Source currently contains Routine
    TRIM(event_category),

    CASE
        WHEN UPPER(TRIM(gate_change_flag)) = 'TRUE' THEN 1
        ELSE 0
    END AS gate_change_flag,

    -- previous_event_timestamp is blank in all records
    NULL AS previous_event_timestamp,

    -- Source gate_open_time, gate_close_time and boarding_completion_time
    -- do not follow a logical order.
    -- Therefore derive realistic timing relative to boarding start.
    --
    -- Gate opens 45 minutes before boarding start
    DATE_SUB(
        STR_TO_DATE(event_timestamp, '%Y-%m-%d %H:%i:%s'),
        INTERVAL 45 MINUTE
    ) AS gate_open_time,

    -- Gate closes 45 minutes after boarding start
    DATE_ADD(
        STR_TO_DATE(event_timestamp, '%Y-%m-%d %H:%i:%s'),
        INTERVAL 45 MINUTE
    ) AS gate_close_time,

    -- Boarding completes 30 minutes after boarding start
    DATE_ADD(
        STR_TO_DATE(event_timestamp, '%Y-%m-%d %H:%i:%s'),
        INTERVAL 30 MINUTE
    ) AS boarding_completion_time

FROM bronze.ops_gate_events
WHERE NULLIF(TRIM(gate_event_id), '') IS NOT NULL;


