import unittest
from clean import clean_column
from tests.spark_test_case import SparkTestCase

class TestCleanCompanyName(SparkTestCase):

    def test_clean_company_name(self):
        # Sample DataFrame with company names
        test_data = self.spark.createDataFrame(
            [
                ("Fresh Fruits Co", "Plastic"),
                ("Fresh Fruits c.o", "Metal"),
                ("Healthy Snacks Co", "Plastic"),
                ("Healthy Snack Co", "Wood"),
            ],
            ["company_name", "crate_type"]
        )

        # Apply cleaning function
        cleaned_df = clean_column(test_data, "company_name")

        # Count the distinct company names
        unique_company_count = cleaned_df.select("materialization_company_name").distinct().count()

        # Assert there are exactly two unique company names
        self.assertEqual(unique_company_count, 2)

if __name__ == "__main__":
    unittest.main()