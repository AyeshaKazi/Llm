CREATE OR REPLACE DATA METRIC FUNCTION calculate_z_score_metric(input_table TABLE (close_value FLOAT, lag_5_days FLOAT, std_dev FLOAT))
RETURNS TABLE (z_score FLOAT)
LANGUAGE SQL
AS
$$
SELECT
    CASE
        WHEN std_dev = 0 OR std_dev IS NULL OR lag_5_days IS NULL THEN NULL
        ELSE (close_value - lag_5_days) / std_dev
    END AS z_score
FROM input_table;
$$;



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


# Call the metric function and turn it into a Snowpark DataFrame
metric_df = session.table_function("custom_lag_zscore_metric").alias("metrics")

# Define the feature view by joining the metric results with the original data (optional)
original_df = session.table("your_table_name").alias("original")
feature_view_df = metric_df.join(original_df, metric_df["date"] == original_df["date"]).select(
    metric_df["date"],
    metric_df["close_value"],
    metric_df["lag_5_days"],
    metric_df["std_dev"],
    metric_df["z_score"]
)

# Save the feature view as a Snowflake table or temporary view, if needed
feature_view_df.write.mode("overwrite").save_as_table("custom_lag_zscore_feature_view")


task_sql = """
CREATE OR REPLACE TASK refresh_custom_lag_zscore_feature_view
WAREHOUSE = '<your_warehouse>'
SCHEDULE = 'USING CRON 0 0 * * *'
AS
REFRESH FEATURE VIEW custom_lag_zscore_feature_view;
"""

# Execute the SQL to create the task
session.sql(task_sql).collect()
