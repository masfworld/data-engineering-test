import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

from challenge.challenge_1 import calculate_crate_distribution

@pytest.fixture(scope="session")
def spark():
    """Fixture to initialize a SparkSession for testing."""
    return SparkSession.builder \
        .appName("TestCalculateCrateDistribution") \
        .master("local[*]") \
        .getOrCreate()

def test_calculate_crate_distribution(spark):
    # Sample data mimicking the input schema
    sample_data = [
        Row(company_id="20dfef10-8f4e-45a1-82fc-123f4ab2a4a5", company_name="Healthy Snacks Co", crate_type="Type A"),
        Row(company_id="20dfef10-8f4e-45a1-82fc-123f4ab2a4a5", company_name="healthy snacks c.o.", crate_type="Type A"),
        Row(company_id="20dfef10-8f4e-45a1-82fc-123f4ab2a4a5", company_name="HEALTHY SNACKS CO", crate_type="Type B"),
        Row(company_id="a1b2c3d4-8f4e-45a1-82fc-9876543210a5", company_name="Other Company Name", crate_type="Type A"),
        Row(company_id="a1b2c3d4-8f4e-45a1-82fc-9876543210a5", company_name="Other Company Name", crate_type="Type A")
    ]

    # Create a PySpark DataFrame from the sample data
    orders_df = spark.createDataFrame(sample_data)

    # Call the function under test
    result_df = calculate_crate_distribution(orders_df)

    # Expected output data
    expected_data = [
        Row(materialized_company_name="Healthy Snacks Co", crate_type="Type A", order_count=2),
        Row(materialized_company_name="Healthy Snacks Co", crate_type="Type B", order_count=1),
        Row(materialized_company_name="Other Company Name", crate_type="Type A", order_count=2)
    ]

    # Create an expected DataFrame
    expected_df = spark.createDataFrame(expected_data)

    # Sort both DataFrames for comparison
    result_df = result_df.orderBy("materialized_company_name", "crate_type", "order_count")
    expected_df = expected_df.orderBy("materialized_company_name", "crate_type", "order_count")

    # Assert that the sorted DataFrames match
    assert result_df.collect() == expected_df.collect()