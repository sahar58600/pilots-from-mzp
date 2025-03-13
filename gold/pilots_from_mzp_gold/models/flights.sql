{{ config(
    materialized='table',
) }}

WITH flights AS (
    SELECT
        flight_number,
        departure_date::date AS departure_date,  -- Casts to date
        flight_date::date AS flight_date,        -- Casts to date
        seats_booked_count,
        crew_members_count,
        fuel_consumption,
        fuel_price
    FROM
        silver.flights
),
flight_schedule AS (
    SELECT
        flight_number,
        departure_date::date AS flight_date,  -- Cast to date
        destination,
        airline
    FROM
        silver.flight_schedule
)

SELECT
    fs.flight_number,
    TO_CHAR(fs.flight_date, 'YYYY-MM-DD') AS departure_date,  -- Ensure the correct column is used
    fs.destination,
    fs.airline,
    TO_CHAR(f.flight_date, 'YYYY-MM-DD') AS flight_date,  -- Keep this as a date
    f.seats_booked_count,
    f.crew_members_count,
    f.fuel_consumption,
    f.fuel_price
FROM
    flight_schedule fs
INNER JOIN
    flights f
ON
    fs.flight_date = f.departure_date  -- Use the correct columns
    AND fs.flight_number = f.flight_number