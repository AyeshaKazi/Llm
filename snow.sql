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



-- Setting context
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE aw_spcs_db;
USE SCHEMA aw_spcs_db.public;

-- Cortex Search
-- Create a preprocessing function to parse the PDF file, extract the text, and chunk the text into smaller pieces for indexing.
CREATE OR REPLACE FUNCTION aw_spcs_db.public.pdf_text_chunker(file_url STRING)
    RETURNS TABLE (chunk VARCHAR)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.9'
    HANDLER = 'pdf_text_chunker'
    PACKAGES = ('snowflake-snowpark-python', 'PyPDF2', 'langchain')
    AS
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2, io
import logging
import pandas as pd

class pdf_text_chunker:

    def read_pdf(self, file_url: str) -> str:
        logger = logging.getLogger("udf_logger")
        logger.info(f"Opening file {file_url}")

        with SnowflakeFile.open(file_url, 'rb') as f:
            buffer = io.BytesIO(f.readall())

        reader = PyPDF2.PdfReader(buffer)
        text = ""
        for page in reader.pages:
            try:
                text += page.extract_text().replace('\n', ' ').replace('\0', ' ')
            except:
                text = "Unable to Extract"
                logger.warn(f"Unable to extract from file {file_url}, page {page}")

        return text

    def process(self, file_url: str):
        text = self.read_pdf(file_url)

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 2000,  # Adjust this as needed
            chunk_overlap = 300,  # Overlap to keep chunks contextual
            length_function = len
        )

        chunks = text_splitter.split_text(text)
        df = pd.DataFrame(chunks, columns=['chunk'])

        yield from df.itertuples(index=False, name=None)
$$;


-- Now let's create a table to hold the parsed data from the PDF files.
CREATE OR REPLACE TABLE aw_spcs_db.public.swiss_faq_chunks AS
    SELECT
        relative_path,
        build_scoped_file_url(@aw_spcs_db.public.aw_spcs_pdf_stage, relative_path) AS file_url,
        -- preserve file title information by concatenating relative_path with the chunk
        CONCAT(relative_path, ': ', func.chunk) AS chunk,
        'English' AS language
    FROM
        directory(@aw_spcs_db.public.aw_spcs_pdf_stage),
        TABLE(aw_spcs_db.public.pdf_text_chunker(build_scoped_file_url(@aw_spcs_db.public.aw_spcs_pdf_stage,
        relative_path))) AS func
        ;

-- Let's check the results
SELECT * FROM aw_spcs_db.public.swiss_faq_chunks;


-- Create a search service over aw_spcs_db.public.swiss_faq_chunks
CREATE OR REPLACE CORTEX SEARCH SERVICE aw_spcs_db.public.aw_spcs_search_svc
    ON chunk
    ATTRIBUTES language
    WAREHOUSE = compute_wh
    TARGET_LAG = '1 hour'
    AS (
    SELECT
        chunk,
        relative_path,
        file_url,
        language
    FROM aw_spcs_db.public.swiss_faq_chunks
    );
