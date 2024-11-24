import logging
from pyspark.sql import SparkSession
from extract import load_orders, load_invoicing_data
from clean import clean_column
from helpers.menu import display_menu, process_menu_choice
from helpers.logging import logging_conf

if __name__ == "__main__":
    # Configure logging
    logging_conf()

    logging.info("Starting PySpark Application")

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("IFCO Data Challenge") \
        .getOrCreate()

    # File paths
    orders_file = "resources/orders.csv"
    invoicing_file = "resources/invoicing_data.json"

    try:
        # Load and clean data
        orders_df = clean_column(load_orders(spark, orders_file), "company_name")
        invoicing_df = load_invoicing_data(spark, invoicing_file)

        # Main menu loop
        while True:
            choice = display_menu()
            if not process_menu_choice(choice, orders_df, invoicing_df):
                break

    except Exception as e:
        logging.critical(f"Unhandled exception: {e}")
        print("An error occurred. Check logs for details.")
    finally:
        spark.stop()
        logging.info("Stopped PySpark Application")