import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import count

logging.getLogger(__name__)

def calculate_crate_distribution(orders_df: DataFrame) -> DataFrame:
    """
    Calculate the distribution of crate types per company.
    
    :param orders_df: PySpark DataFrame containing orders data.
    :return: PySpark DataFrame with company_name, crate_type, and counts.
    """
    try:
        logging.info("Calculating crate distribution.")
        # Group by company_name and crate_type, count occurrences
        distribution = (
            orders_df.groupBy("company_name", "crate_type")
            .agg(count("*").alias("order_count"))
        )
        logging.info("Successfully calculated crate distribution.")
        return distribution
    except Exception as e:
        logging.error(f"Error in calculate_crate_distribution: {e}")
        raise

def challenge_1(orders_df: DataFrame):
    logging.info("Running Challenge 1: Distribution of Crate Type per Company")
    result = calculate_crate_distribution(orders_df)
    result.show(result.count(), truncate=False)  # Display the results