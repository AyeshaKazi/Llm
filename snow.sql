-- Setting context
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE aw_spcs_db;
USE SCHEMA aw_spcs_db.public;

-- For use when creating a semantic model through the generator
SELECT CURRENT_ACCOUNT_LOCATOR();  -- <your account locator>
-- SNOWFLAKE_HOST 
-- Account Identifier - ABCD.WXYZ
-- Account URL - https://<your account locator>.snowflakecomputing.com

-- Check semantic model in stage
LIST @aw_spcs_semantics_stage;

-- All_Flights
CREATE OR REPLACE TABLE aw_spcs_db.public.all_flight_details AS
SELECT bkgs.book_ref, bkgs.book_date, bkgs.total_amount,
tkts.passenger_id, tkts.ticket_no,
tkt_flts.fare_conditions, tkt_flts.flight_id,
flts.flight_no, flts.status, flts.departure_airport, flts.arrival_airport, flts.scheduled_departure, flts.scheduled_arrival, flts.actual_departure, flts.actual_arrival,
arcfts.aircraft_code, arcfts.model, arcfts.range,
brd_ps.boarding_no, brd_ps.seat_no
FROM aw_spcs_db.public.bookings bkgs  -- 262788 rows
JOIN aw_spcs_db.public.tickets tkts ON bkgs.book_ref = tkts.book_ref  -- More than 1 ticket per booking reference, 366733 rows
JOIN aw_spcs_db.public.ticket_flights tkt_flts ON tkts.ticket_no = tkt_flts.ticket_no  -- More than 1 flight per ticket number, 1045726 rows
JOIN aw_spcs_db.public.flights flts ON tkt_flts.flight_id = flts.flight_id  -- No change, getting joined flight details
JOIN aw_spcs_db.public.aircrafts_data arcfts ON flts.aircraft_code = arcfts.aircraft_code  -- No change, getting joined aircraft details
-- Here we use Left join as some flights are scheduled and don't have a flight, boarding, and seats details
LEFT JOIN aw_spcs_db.public.boarding_passes brd_ps ON flts.flight_id = brd_ps.flight_id AND tkts.ticket_no = brd_ps.ticket_no  -- No change, joined boarding pass details
LEFT JOIN aw_spcs_db.public.seats sts ON flts.aircraft_code = sts.aircraft_code AND brd_ps.seat_no = sts.seat_no  -- No change, joined seats details
ORDER BY bkgs.book_ref, tkts.ticket_no, tkt_flts.flight_id, flts.aircraft_code
;

SELECT * FROM aw_spcs_db.public.all_flight_details;
