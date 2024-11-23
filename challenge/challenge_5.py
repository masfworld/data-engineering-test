import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, split, collect_set, array_sort, concat_ws, collect_list

logging.getLogger(__name__)

def get_companies_with_sales_owners(orders_df: DataFrame) -> DataFrame:
    """
    Create a DataFrame with materialized_company_name, concatenated company_ids, and a sorted list of unique sales owners.

    :param orders_df: PySpark DataFrame containing orders data.
    :return: PySpark DataFrame with columns materialized_company_name, concatenated_company_ids, and list_salesowners.
    """
    try:
        logging.info("Generating DataFrame of companies with sales owners and concatenated company IDs.")

        # Explode salesowners to normalize them
        exploded_df = orders_df.withColumn(
            "sales_owner",
            explode(split(col("salesowners"), ", "))
        )

        # Aggregate sales owners and company IDs grouped by materialized_company_name
        # We can have more than one company_id per company based on the description of the challenge
        companies_with_salesowners = exploded_df.groupBy("materialization_company_name").agg(
            array_sort(collect_set(col("sales_owner"))).alias("sorted_salesowners"),
            concat_ws(", ", collect_set(col("company_id"))).alias("company_id")
        )

        # Convert sorted sales owners array to a comma-separated string
        result = companies_with_salesowners.withColumn(
            "list_salesowners",
            concat_ws(", ", col("sorted_salesowners"))
        ).select("company_id",col("materialization_company_name").alias("company_name"), "list_salesowners")

        logging.info("Successfully generated DataFrame of companies with sales owners and concatenated company IDs.")
        return result
    except Exception as e:
        logging.error(f"Error in get_companies_with_sales_owners: {e}")
        raise


def challenge_5(orders_df: DataFrame):
    logging.info("Running Challenge 5: DataFrame of Companies with Sales Owners and Concatenated Company IDs")
    df_3 = get_companies_with_sales_owners(orders_df)
    df_3.show(truncate=False)