import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, first

logging.getLogger(__name__)

def calculate_crate_distribution(orders_df: DataFrame) -> DataFrame:
    """
    Calculate the distribution of crate types per materialized company name.

    :param orders_df: PySpark DataFrame containing orders data.
    :return: PySpark DataFrame with materialized_company_name, crate_type, and counts.
    """
    try:
        # Materialize unique company names by company_id
        # This is because there are multiple names for the same company_id
        # So I'm grouping based on company_id because my assumption is that id is correct
        # And company_name contains typo errors
        materialized_companies = (
            orders_df.groupBy("company_id")
            .agg(first("company_name", ignorenulls=True).alias("materialized_company_name"))
        )

        # Join materialized names back to the original DataFrame
        normalized_df = orders_df.join(materialized_companies, on="company_id", how="inner")

        # Group only by materialized_company_name and crate_type
        distribution = (
            normalized_df.groupBy("materialized_company_name", "crate_type")
            .agg(count("*").alias("order_count"))
        )

        return distribution
    except Exception as e:
        raise ValueError(f"Error in calculate_crate_distribution: {e}")


def challenge_1(orders_df: DataFrame):
    """
    Run Challenge 1: Distribution of Crate Type per Company.

    :param orders_df: PySpark DataFrame containing orders data.
    """
    logging.info("Running Challenge 1: Distribution of Crate Type per Company")
    result = calculate_crate_distribution(orders_df)
    result.show(result.count(), truncate=False)  # Display the results