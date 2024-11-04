metric_function_sql = """
CREATE OR REPLACE DATA METRIC FUNCTION custom_lag_zscore_metric(close_value FLOAT)
RETURNS TABLE (
    date DATE,
    close_value FLOAT,
    lag_5_days FLOAT,
    std_dev FLOAT,
    z_score FLOAT
)
LANGUAGE SQL
AS
$$
WITH lag_data AS (
    SELECT
        date,
        close_value,
        LAG(close_value, 5) OVER (ORDER BY date) AS lag_5_days
    FROM your_table_name
),
std_dev_data AS (
    SELECT
        date,
        close_value,
        lag_5_days,
        STDDEV_SAMP(close_value) OVER (ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS std_dev
    FROM lag_data
)
SELECT
    date,
    close_value,
    lag_5_days,
    std_dev,
    CASE
        WHEN std_dev = 0 THEN NULL
        ELSE (close_value - lag_5_days) / std_dev
    END AS z_score
FROM std_dev_data;
$$;
"""

# Execute the SQL to create the data metric function
session.sql(metric_function_sql).collect()
