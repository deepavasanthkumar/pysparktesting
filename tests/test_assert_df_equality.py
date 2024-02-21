from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *
import pytest
from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F

def test_remove_non_word_characters_long_error():

    spark = (SparkSession.builder.master("local").appName("PySpark Test Framework").getOrCreate())
    def remove_non_word_characters(col):
        return F.regexp_replace(col, "[^\\w\\s]+", "")
    source_data = [
        ("matt7",),
        ("bill&",),
        ("isabela*",),
        (None,)
    ]
    source_df = spark.createDataFrame(source_data, ["name"])

    actual_df = source_df.withColumn(
        "clean_name",
        remove_non_word_characters(F.col("name"))
    )

    expected_data = [
        ("matt7", "matt"),
        ("bill&", "bill"),
        ("isabela*", "isabela"),
        (None, None)
    ]
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])

    assert_df_equality(actual_df, expected_df)
    
