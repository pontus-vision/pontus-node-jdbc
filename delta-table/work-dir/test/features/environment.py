from behave import fixture, use_fixture
from pyspark.sql import SparkSession

import sys
import os

# Get the absolute path to the 'udl' directory
udl_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Define the Spark session as a fixture
@fixture
def spark_context(context):
    context.spark = SparkSession.builder.appName("MySparkApp").getOrCreate()
    context.sc = context.spark.sparkContext
    # to be able to use paths.py in job's files
    context.sc.addPyFile(os.path.join(udl_dir,'udl', 'udl.py') )

    yield context.spark
    context.spark.stop()  # Clean up after all scenarios

# Use the fixture in all scenarios
def before_all(context):
    use_fixture(spark_context, context)

# Optionally, you can add other fixtures or setup steps here
