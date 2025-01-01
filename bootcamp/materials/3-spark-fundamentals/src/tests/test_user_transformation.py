from chispa.dataframe_comparer import *
from pyspark.sql.functions import to_date
from ..jobs.user_trans_job import do_users_transformation, create_homework_db, create_users_cumulated_table
from collections import namedtuple
from pyspark.sql.functions import array
from pyspark.sql.types import *
from datetime import datetime

Events = namedtuple("Events", "url referrer user_id device host event_time")
UsersCumulative = namedtuple("UsersCumulative", "user_id dates_active date")


def test_users_transformations(spark):
    source_data = [
        Events(
            url="/",
            referrer="http://admin.zachwilson.tech",
            user_id='1',
            device='100', 
            host='www.zachwilson.tech',
            event_time='2023-01-01 04:43:49.204000'
        ),
        Events(
            url="/robots.txt",
            referrer="http://admin.zachwilson.tech",
            user_id='2',
            device='101',
            host='www.zachwilson.tech',
            event_time='2023-01-01 04:43:49.204000'
        ),
        Events(
            url="/robots.txt",
            referrer="http://admin.zachwilson.tech",
            user_id='3', 
            device='102', 
            host='www.zachwilson.tech',
            event_time='2023-01-10 04:43:49.204000'
        ),
        Events(
            url="/ads.txt",
            referrer="http://admin.zachwilson.tech",
            user_id='4',
            device='103', 
            host='www.zachwilson.tech',
            event_time='2023-01-10 04:43:49.204000'
        )
    ]
    source_df = spark.createDataFrame(source_data)
    create_homework_db(spark)
    create_users_cumulated_table(spark)

    actual_df = do_users_transformation(spark, source_df)

    expected_data = [
        UsersCumulative("2", [datetime(2023, 1, 1)], datetime(2023, 1, 1)),
        UsersCumulative("1", [datetime(2023, 1, 1)], datetime(2023, 1, 1))
    ]

    schema = StructType(
        [
            StructField("user_id", StringType(), True), 
            StructField("dates_active", ArrayType(DateType()), True), 
            StructField('date', DateType(), True)
        ]
    )
    expected_df = spark.createDataFrame(expected_data, schema)
    assert_df_equality(actual_df, expected_df)
