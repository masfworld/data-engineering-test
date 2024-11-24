import unittest
from challenge.challenge_1 import calculate_crate_distribution
from tests.spark_test_case import SparkTestCase

class TestChallenge1(SparkTestCase):

    def test_calculate_crate_distribution(self):
        orders_data = self.spark.createDataFrame(
            [
                ("Company A", "Plastic"),
                ("Company A", "Wood"),
                ("Company B", "Plastic"),
                ("Company B", "Metal"),
                ("Company A", "Plastic"),
            ],
            ["materialization_company_name", "crate_type"]
        )

        result = calculate_crate_distribution(orders_data)
        expected = self.spark.createDataFrame(
            [
                ("Company A", "Plastic", 2),
                ("Company A", "Wood", 1),
                ("Company B", "Metal", 1),
                ("Company B", "Plastic", 1),
            ],
            ["company_name", "crate_type", "order_count"]
        )

        # Sort both DataFrames to ensure consistent order before comparison
        result_sorted = result.orderBy("company_name", "crate_type").collect()
        expected_sorted = expected.orderBy("company_name", "crate_type").collect()

        # Compare the sorted rows
        self.assertEqual(result_sorted, expected_sorted)

if __name__ == "__main__":
    unittest.main()