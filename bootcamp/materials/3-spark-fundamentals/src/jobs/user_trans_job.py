from pyspark.sql import SparkSession

create_db = """
CREATE DATABASE IF NOT EXISTS homework;
"""


create_user_cumulated_table = """
CREATE TABLE IF NOT EXISTS homework.users_cumulated (
    user_id STRING,
    -- The list of dates in the past where the user was active
    dates_active Array<DATE>,
    -- Current data for the user
    date DATE
);
"""

user_cumulated_query = """
WITH yesterday AS (
    SELECT
        *
    FROM homework.users_cumulated
    WHERE date = to_date('2022-12-31')
),
    today AS (
            SELECT
                  CAST(user_id AS STRING),
                  CAST(event_time AS DATE) as date_active
              FROM events
              WHERE
                  CAST(event_time AS DATE) = to_date('2023-01-01')
                    AND user_id IS NOT NULL
              GROUP BY user_id, CAST(event_time AS DATE)
    )
SELECT
    COALESCE(t.user_id, y.user_id) as user_id,
    CASE WHEN y.dates_active IS NULL
            THEN array(t.date_active)
        WHEN t.date_active IS NULL
            THEN y.dates_active
        ELSE concat(array(t.date_active), y.dates_active)
        END as dates_active,
    COALESCE(t.date_active, date_add(y.date, 1)) AS date
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
"""

def create_homework_db(spark):
    spark.sql(create_db)


def create_users_cumulated_table(spark):
    spark.sql(create_user_cumulated_table)


def do_users_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("events")
    return spark.sql(user_cumulated_query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("spark_testing") \
      .getOrCreate()
    output_df = do_users_transformation(spark, spark.table("events_df"))
    output_df.write.mode("overwrite").insertInto("homework.users_cumulated")