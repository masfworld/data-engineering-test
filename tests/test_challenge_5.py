import unittest
from pyspark.sql import Row
from challenge.challenge_5 import get_companies_with_sales_owners
from tests.spark_test_case import SparkTestCase


class TestGetCompaniesWithSalesOwners(SparkTestCase):

    def test_get_companies_with_sales_owners(self):
        # Sample input data
        data = [
            Row(company_id="20dfef10-8f4e-45a1-82fc-123f4ab2a4a5", materialization_company_name="Healthy Snacks Co", salesowners="Alice, Bob"),
            Row(company_id="20dfef10-8f4e-45a1-82fc-123f4ab2a4a5", materialization_company_name="Healthy Snacks Co", salesowners="Alice, Charlie"),
            Row(company_id="9f4ac15d-3b0c-4c8e-a6d2-45ac78ef12ab", materialization_company_name="Tasty Treats", salesowners="Bob, Dana"),
            Row(company_id="20dfef10-8f4e-45a1-82fc-123f4ab2a4a5", materialization_company_name="Healthy Snacks Co", salesowners="Alice, Bob")
        ]
        input_df = self.spark.createDataFrame(data)

        # Expected output data
        expected_data = [
            Row(company_id="20dfef10-8f4e-45a1-82fc-123f4ab2a4a5",
                company_name="Healthy Snacks Co",
                list_salesowners="Alice, Bob, Charlie"),
            Row(company_id="9f4ac15d-3b0c-4c8e-a6d2-45ac78ef12ab",
                company_name="Tasty Treats",
                list_salesowners="Bob, Dana")
        ]
        expected_df = self.spark.createDataFrame(expected_data) \
                        .orderBy("company_id", "company_name", "list_salesowners")

        # Run the function
        result_df = get_companies_with_sales_owners(input_df) \
                        .orderBy("company_id", "company_name", "list_salesowners")

        # Assert that the result matches the expected output
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_test_multiple_company_id(self):
        # Sample input data
        data = [
            Row(company_id="abc", materialization_company_name="Healthy Snacks Co", salesowners="Alice, Bob"),
            Row(company_id="def", materialization_company_name="Healthy Snacks Co", salesowners="Alice, Charlie")
        ]
        input_df = self.spark.createDataFrame(data)

        # Expected output data
        expected_data = [
            Row(company_id="def, abc",
                company_name="Healthy Snacks Co",
                list_salesowners="Alice, Bob, Charlie"),
        ]
        expected_df = self.spark.createDataFrame(expected_data)

        # Run the function
        result_df = get_companies_with_sales_owners(input_df)

        # Assert that the result matches the expected output
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))


if __name__ == '__main__':
    unittest.main()