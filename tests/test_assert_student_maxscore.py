
from pyspark.sql import Row
from pyspark.sql import SparkSession
import pytest
from pyspark.sql import functions as F
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import *

def create_student_data(spark):
    schema = StructType([
        StructField("name", StringType()),
        StructField("subject", StringType()),
        StructField("score", IntegerType())
    ])
    data = [
        {"name": "Ajay", "subject": "English", "score": 90},
        {"name": "Ajay", "subject": "Science", "score": 75},
        {"name": "Benjamin", "subject": "English", "score": 87},
        {"name": "Benjamin", "subject": "Science", "score": 40},
        {"name": "Carl", "subject": "English", "score": 68},
        {"name": "Carl", "subject": "Science", "score": 93},
        {"name": "Derrik", "subject": "English", "score": 81},
        {"name": "Derrik", "subject": "Science", "score": 94},
    ]
    df = spark.createDataFrame([Row(**x) for x in data], schema)
    df.cache()
    df.count()
    return df.groupBy("subject").agg(F.max("score").alias("max_score"))
        



def create_expected_max_score(spark):
    schema = StructType([
        StructField("subject", StringType()),
        StructField("max_score", IntegerType())
    ])
    expected_data = [
        {"subject": "English", "max_score": 90},
        {"subject": "Science", "max_score": 94},
    ]
    expected_result_df = spark.createDataFrame(
        [Row(**x) for x in expected_data], 
        schema
    )
    return expected_result_df

def create_expected_max_score_failed(spark):
    schema = StructType([
        StructField("subject", StringType()),
        StructField("max_score", IntegerType())
    ])
    expected_data = [
        {"subject": "English", "max_score": 90},
        {"subject": "Science", "max_score": 90},
    ]
    expected_result_df = spark.createDataFrame(
        [Row(**x) for x in expected_data], 
        schema
    )
    return expected_result_df

def test_get_top_score():
    spark = (SparkSession.builder.master("local").appName("PySpark Test Framework - Subject Max Score").getOrCreate())
    expected_result_df = create_expected_max_score(spark)
    result = create_student_data(spark)
    assert_df_equality(
        result, 
        expected_result_df, 
        ignore_column_order=True, 
        ignore_row_order=True
    )
    

def test_get_top_score_failure():
    spark = (SparkSession.builder.master("local").appName("PySpark Test Framework - Subject Max Score - Failure").getOrCreate())
    expected_result_df = create_expected_max_score_failed(spark)
    result = create_student_data(spark)
    assert_df_equality(
        result, 
        expected_result_df, 
        ignore_column_order=True, 
        ignore_row_order=True
    )
