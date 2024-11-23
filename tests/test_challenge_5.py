import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from challenge.challenge_5 import get_companies_with_sales_owners

class TestChallenge5(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        # Sample orders data
        self.orders_data = self.spark.createDataFrame([
            Row(order_id="1", salesowners="Alice, Bob"),
            Row(order_id="2", salesowners="Bob, Charlie"),
            Row(order_id="3", salesowners="Alice, Bob, Dave"),
            Row(order_id="4", salesowners="Eve"),
            Row(order_id="5", salesowners="Eve, Alice")
        ])

        # Sample invoices data
        self.invoices_data = self.spark.createDataFrame([
            Row(order_id="1", company_id="C1", company_name="Alpha Inc"),
            Row(order_id="2", company_id="C1", company_name="Alpha Inc"),
            Row(order_id="3", company_id="C3", company_name="Beta LLC"),
            Row(order_id="4", company_id="C4", company_name="Gamma Co"),
            Row(order_id="5", company_id="C4", company_name="Gamma Co")  # Multiple orders for Gamma Co
        ])

    def tearDown(self):
        self.spark.stop()

    def test_get_companies_with_sales_owners(self):
        result = get_companies_with_sales_owners(self.orders_data, self.invoices_data)

        expected_data = self.spark.createDataFrame([
            Row(company_id="C1", company_name="Alpha Inc", list_salesowners="Alice, Bob, Charlie"),
            Row(company_id="C3", company_name="Beta LLC", list_salesowners="Alice, Bob, Dave"),
            Row(company_id="C4", company_name="Gamma Co", list_salesowners="Alice, Eve")
        ])

        result_sorted = result.orderBy("company_id").collect()
        expected_sorted = expected_data.orderBy("company_id").collect()
        self.assertEqual(result_sorted, expected_sorted)