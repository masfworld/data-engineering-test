import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, explode, lit, when, round, expr, sum as _sum

logging.getLogger(__name__)

def calculate_commissions(orders_df: DataFrame, invoices_df: DataFrame) -> DataFrame:
    """
    Calculate sales team commissions based on the net invoiced value.

    :param orders_df: PySpark DataFrame containing orders data.
    :param invoices_df: PySpark DataFrame containing invoice data with net invoiced values.
    :return: PySpark DataFrame with columns sales_owner and total_commission (sorted by descending commission).
    """
    try:
        logging.info("Calculating commissions for sales team.")

        # Join orders with invoices to get net_invoiced value
        orders_with_invoices = orders_df.join(
            invoices_df.withColumn(
                "net_invoiced",
                col("gross_value").cast("double") * (1 - col("vat").cast("double") / 100)
            ),
            on=orders_df["order_id"] == invoices_df["order_id"],
            how="inner"
        ).select( # Adding all columns from orders + net_invoiced
            orders_df["*"],
            col("net_invoiced")
        )

        # Explode salesowners into individual rows
        exploded_sales = orders_with_invoices.withColumn(
            "sales_owner",
            explode(split(col("salesowners"), ", "))
        )

        # Assign roles and calculate commission based on rank
        commissions = exploded_sales.withColumn(
            "rank",
            expr("row_number() over (partition by order_id order by array_position(split(salesowners, ', '), sales_owner))")
        ).withColumn(
            "commission",
            when(col("rank") == 1, col("net_invoiced") * lit(0.06))
            .when(col("rank") == 2, col("net_invoiced") * lit(0.025))
            .when(col("rank") == 3, col("net_invoiced") * lit(0.0095))
            .otherwise(lit(0))
        )

        # Aggregate commissions by sales owner
        total_commissions = commissions.groupBy("sales_owner").agg(
            round((_sum(col("commission")) / 100), 2).alias("total_commission")  # Aggregate and convert cents to euros
        ).orderBy(col("total_commission").desc())

        logging.info("Successfully calculated commissions.")
        return total_commissions
    except Exception as e:
        logging.error(f"Error in calculate_commissions: {e}")
        raise


def challenge_4(orders_df: DataFrame, invoices_df: DataFrame):
    logging.info("Running Challenge 4: Calculation of Sales Team Commissions")
    df_commissions = calculate_commissions(orders_df, invoices_df)
    df_commissions.show(truncate=False)