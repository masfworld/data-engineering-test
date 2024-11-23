import unittest
from pyspark.sql import SparkSession
from clean import clean_column

class TestCleanCompanyName(unittest.TestCase):
    def setUp(self):
        # Create SparkSession
        self.spark = SparkSession.builder.master("local").appName("TestCleanCompanyName").getOrCreate()

        # Sample DataFrame with company names
        self.test_data = self.spark.createDataFrame(
            [
                ("Fresh Fruits Co", "Plastic"),
                ("Fresh Fruits c.o", "Metal"),
                ("Healthy Snacks Co", "Plastic"),
                ("Healthy Snack Co", "Wood"),
            ],
            ["company_name", "crate_type"]
        )

    def tearDown(self):
        # Stop SparkSession
        self.spark.stop()

    def test_clean_company_name(self):
        # Apply cleaning function
        cleaned_df = clean_column(self.test_data, "company_name")

        # Count the distinct company names
        unique_company_count = cleaned_df.select("company_name").distinct().count()

        # Assert there are exactly two unique company names
        self.assertEqual(unique_company_count, 2)

if __name__ == "__main__":
    unittest.main()