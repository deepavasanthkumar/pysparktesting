We all know the difficult part of pyspark is to get a validate it with proper test execution.
This repo is to aid testing pyspark with the help of chispa testing framework.
I have given a basic example which can be expanded or modified as the per the need of the business problem.


# pyspark testing using colab and chispa testing framework
Code utilities to test pyspark code

The step by step way of executing test scripts using colab
Poetry is being used packaging utility.

# configure poetry in colab
Configure poetry to use in colab
![image](https://github.com/deepavasanthkumar/pysparktesting/assets/6638817/32a34c6d-5ebe-45d0-95ad-5a9fe189095e)

Give the required details to configure poetry
![image](https://github.com/deepavasanthkumar/pysparktesting/assets/6638817/c782c85f-38f5-46a4-af4d-7fc3fabeaa15)


# Write the test scripts 
using chispa for data frame equality checks in spark.
we can directly compare two dataframes

![image](https://github.com/deepavasanthkumar/pysparktesting/assets/6638817/359f574d-0ceb-4940-8569-0895b2ff5e8c)

![image](https://github.com/deepavasanthkumar/pysparktesting/assets/6638817/d1923900-e830-490c-81f9-8b6bd1f42c10)


# Execute and evaluate the results

like this:
    expected_df = spark.createDataFrame(expected_data, ["name", "clean_name"])
    assert_df_equality(actual_df, expected_df, underline_cells=True)

We can get the summary of tests execution and also details on the failed tests.
![image](https://github.com/deepavasanthkumar/pysparktesting/assets/6638817/06adfd9c-9d2c-4ef6-9f7f-c997eef6bdee)



