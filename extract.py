from pyspark.sql.functions import from_json, coalesce, col, lit, explode, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging


def load_orders(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load the orders CSV file as a PySpark DataFrame.
    
    :param spark: SparkSession instance.
    :param file_path: Path to the orders CSV file.
    :return: PySpark DataFrame.
    """
    logging.info(f"Loading orders data from {file_path}.")
    orders_df = spark.read.csv(file_path, header=True, escape='"', inferSchema=True, sep=";")

    # Define the schema for the JSON array
    contact_schema = ArrayType(StructType([
        StructField("contact_name", StringType(), True),
        StructField("contact_surname", StringType(), True),
        StructField("city", StringType(), True),
        StructField("cp", StringType(), True)
    ]))
    orders_cleaned = orders_df.withColumn(
        "contact_data",
        coalesce(col("contact_data"), lit('[{ "contact_name":"John", "contact_surname":"Doe", "city":"Unknown", "cp":"UNK00"}]')) # Set default values when contact_data is null
    )
    # Parse JSON in the `contact_data` column
    orders_parsed = orders_cleaned.withColumn(
        "contact_array",
        from_json(col("contact_data"), contact_schema)
    )

    return orders_parsed


def load_invoicing_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load the invoicing JSON file as a PySpark DataFrame.

    :param spark: SparkSession instance.
    :param file_path: Path to the invoicing JSON file.
    :return: PySpark DataFrame with flattened invoicing data.
    """
    logging.info(f"Loading invoicing data from {file_path}.")

    # Define schema for invoices (optional, included for clarity)
    invoice_schema = StructType([
        StructField("id", StringType(), True),
        StructField("orderId", StringType(), True),
        StructField("companyId", StringType(), True),
        StructField("grossValue", StringType(), True),
        StructField("vat", StringType(), True)
    ])

    # Read the JSON file
    raw_df = spark.read.option("multiline", "true").json(file_path)

    # Flatten the nested structure
    invoices_df = raw_df.select(
        explode(col("data.invoices")).alias("invoice")
    )

    # Select and rename columns for clarity
    invoices_df = invoices_df.select(
        col("invoice.orderId").alias("order_id"),
        col("invoice.companyId").alias("company_id"),
        col("invoice.grossValue").alias("gross_value"),
        col("invoice.vat").alias("vat")
    )

    logging.info("Successfully loaded and flattened invoicing data.")
    return invoices_df