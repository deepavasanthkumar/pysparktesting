# pyspark testing using colab and chispa testing framework
Code utilities to test pyspark code

The step by step way of executing test scripts using colab
Poetry is being used packaging utility.

# configure poetry in colab
Configure poetry to use in colab

# Write the test scripts 
using chispa for data frame equality checks in spark.
we can directly compare two dataframes


# Execute and evaluate the results

like this:
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])
    assert_df_equality(actual_df, expected_df, underline_cells=True)

We can get the summary of tests execution and also details on the failed tests.
![image](https://github.com/deepavasanthkumar/pysparktesting/assets/6638817/06adfd9c-9d2c-4ef6-9f7f-c997eef6bdee)



