import unittest
from pyspark.sql import SparkSession
from challenge.challenge_3 import get_orders_with_contact_address
from pyspark.sql import Row

class TestChallenge3(unittest.TestCase):
    def setUp(self):
        # Create SparkSession
        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
        
        # Sample orders data
        self.orders_data = self.spark.createDataFrame(
            [
                ("1", [Row(contact_name="Diego", contact_surname="Leon", city="Chicago", cp="12345")]),
                ("2", [Row(contact_name="Maria", contact_surname="Lopez", city="Calcutta", cp=None)]),
                ("3", [Row(contact_name="John", contact_surname="Doe", city=None, cp=None)]),
                ("4", [Row(contact_name="Miguel", contact_surname=None, city="Gotham", cp="11111")]),
                ("5", [Row(contact_name="Mateo", contact_surname="Hernandez", city=None, cp="65432")])
            ],
            ["order_id", "contact_array"]
        )

    def tearDown(self):
        self.spark.stop()

    def test_get_orders_with_contact_address(self):
        # Generate the result
        result = get_orders_with_contact_address(self.orders_data)

        # Expected output
        expected = self.spark.createDataFrame(
            [
                ("1", "Chicago, 12345"),
                ("2", "Calcutta, UNK00"),
                ("3", "Unknown, UNK00"),
                ("4", "Gotham, 11111"),
                ("5", "Unknown, 65432")
            ],
            ["order_id", "contact_address"]
        )

        # Sort and compare
        result_sorted = result.orderBy("order_id").collect()
        expected_sorted = expected.orderBy("order_id").collect()
        self.assertEqual(result_sorted, expected_sorted)

if __name__ == "__main__":
    unittest.main()