import unittest
from pyspark.sql import Row
from challenge.challenge_2 import get_orders_with_full_name
from tests.spark_test_case import SparkTestCase

class TestChallenge2(SparkTestCase):

    def test_get_orders_with_full_name(self):
        # Sample orders data with contact_array instead of contact_data
        orders_data = self.spark.createDataFrame(
            [
                ("1", [Row(contact_name="Diego", contact_surname="Leon", city="Chicago", cp="12345")]),
                ("2", [Row(contact_name="Maria", contact_surname="Lopez", city="Calcutta", cp="56789")]),
                ("3", [Row(contact_name="", contact_surname="", city="Unknown", cp="00000")]),
                ("4", [Row(contact_name="Miguel", contact_surname=None, city="Gotham", cp="11111")]),
                ("5", [Row(contact_name="Mateo", contact_surname="Hernandez", city="Gotham", cp=None)]),
            ],
            ["order_id", "contact_array"]
        )

        # Generate the result
        result = get_orders_with_full_name(orders_data)

        # Expected output
        expected = self.spark.createDataFrame(
            [
                ("1", "Diego Leon"),
                ("2", "Maria Lopez"),
                ("3", "John Doe"),
                ("4", "John Doe"),
                ("5", "Mateo Hernandez"),
            ],
            ["order_id", "contact_full_name"]
        )

        # Sort and compare
        result_sorted = result.orderBy("order_id").collect()
        expected_sorted = expected.orderBy("order_id").collect()
        self.assertEqual(result_sorted, expected_sorted)

if __name__ == "__main__":
    unittest.main()