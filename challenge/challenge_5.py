import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, collect_set, array_sort, concat_ws

logging.getLogger(__name__)

def get_companies_with_sales_owners(orders_df: DataFrame, invoices_df: DataFrame) -> DataFrame:
    """
    Create a DataFrame with company_id, company_name, and a sorted list of unique sales owners.

    :param orders_df: PySpark DataFrame containing orders data.
    :param invoices_df: PySpark DataFrame containing invoice data with company information.
    :return: PySpark DataFrame with columns company_id, company_name, and list_salesowners.
    """
    try:
        logging.info("Generating DataFrame of companies with sales owners.")

        # Explode salesowners and collect unique sorted names
        companies_with_salesowners = orders_df.withColumn(
            "sales_owner",
            explode(split(col("salesowners"), ", "))
        ).groupBy("company_id", "company_name").agg(
            array_sort(collect_set(col("sales_owner"))).alias("sorted_salesowners")
        )

        # Convert array to comma-separated string
        result = companies_with_salesowners.withColumn(
            "list_salesowners",
            concat_ws(", ", col("sorted_salesowners"))
        ).select("company_id", "company_name", "list_salesowners")

        logging.info("Successfully generated DataFrame of companies with sales owners.")
        return result
    except Exception as e:
        logging.error(f"Error in get_companies_with_sales_owners: {e}")
        raise


def challenge_5(orders_df: DataFrame, invoices_df: DataFrame):
    logging.info("Running Challenge 5: DataFrame of Companies with Sales Owners")
    df_3 = get_companies_with_sales_owners(orders_df, invoices_df)
    df_3.show(truncate=False)