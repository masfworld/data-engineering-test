

import unittest
from pyspark.sql import SparkSession


class SparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        """
        Set up a shared Spark session for all test cases.
        This is executed once before running any test in the child class.
        """
        self.spark = SparkSession.builder.master("local").appName("Test1").getOrCreate()

    @classmethod
    def tearDownClass(self):
        """
        Stop the shared Spark session.
        This is executed once after all tests in the child class are completed.
        """
        self.spark.stop()