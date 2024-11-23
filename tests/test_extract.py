import unittest
from pyspark.sql import SparkSession
from extract import load_orders
import tempfile
import os


class TestLoadOrders(unittest.TestCase):
    def setUp(self):
        # Create SparkSession
        self.spark = SparkSession.builder.master("local").appName("TestLoadOrders").getOrCreate()

        # Temporary file setup
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".csv")
        self.temp_file.write(
            """order_id;date;company_id;company_name;crate_type;contact_data;salesowners
f47ac10b-58cc-4372-a567-0e02b2c3d479;29.01.22;1e2b47e6-499e-41c6-91d3-09d12dddfbbd;Fresh Fruits Co;Plastic;"[{ ""contact_name"":""Curtis"", ""contact_surname"":""Jackson"", ""city"":""Chicago"", ""cp"": ""12345""}]";Leonard Cohen
"""
        )
        self.temp_file.close()

    def tearDown(self):
        # Cleanup temporary file
        os.unlink(self.temp_file.name)
        self.spark.stop()

    def test_load_orders_contact_array(self):
        # Call the function
        result = load_orders(self.spark, self.temp_file.name)

        # Extract a few fields to compare
        result_data = result.select("order_id", "contact_array").collect()

        # Expected values for specific fields
        expected_data = [
            {
                "order_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                "contact_array": [{"contact_name": "Curtis", "contact_surname": "Jackson", "city": "Chicago", "cp": "12345"}],
            }
        ]

        # We would need to order the array if we add more rows as the order might be different betwee expected and result
        for row, expected in zip(result_data, expected_data):
            self.assertEqual(row["order_id"], expected["order_id"])
            # Access specific fields in contact_array
            self.assertEqual(row["contact_array"][0]["contact_name"], expected["contact_array"][0]["contact_name"])
            self.assertEqual(row["contact_array"][0]["contact_surname"], expected["contact_array"][0]["contact_surname"])
            self.assertEqual(row["contact_array"][0]["city"], expected["contact_array"][0]["city"])
            self.assertEqual(row["contact_array"][0]["cp"], expected["contact_array"][0]["cp"])


if __name__ == "__main__":
    unittest.main()