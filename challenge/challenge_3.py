import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, concat_ws, explode

logging.getLogger(__name__)

def get_orders_with_contact_address(orders_df: DataFrame) -> DataFrame:
    """
    Create a DataFrame with order_id and contact_address.
    Using placeholders whtn city or postal codes are missing:
      - City: "Unknown"
      - Postal code: "UNK00"

    :param orders_df: PySpark DataFrame containing orders data.
    :return: PySpark DataFrame with columns order_id and contact_address.
    """
    try:
        logging.info("Creating DataFrame with contact address.")

        # Explode the JSON array to get individual contact records
        orders_exploded = orders_df.withColumn("contact", explode("contact_array"))

        # Extract city and postal code (cp) from the parsed JSON
        orders_with_address = orders_exploded.withColumn(
            "contact_city",
            when(col("contact.city").isNotNull(), col("contact.city")).otherwise(lit("Unknown"))
        ).withColumn(
            "contact_cp",
            when(col("contact.cp").isNotNull(), col("contact.cp")).otherwise(lit("UNK00"))
        )

        # Format the address
        result = orders_with_address.withColumn(
            "contact_address",
            concat_ws(", ", col("contact_city"), col("contact_cp"))
        ).select("order_id", "contact_address")

        logging.info("Successfully created DataFrame with contact address.")
        return result
    except Exception as e:
        logging.error(f"Error in get_orders_with_contact_address: {e}")
        raise


def challenge_3(orders_df: DataFrame):
    logging.info("Running Challenge 3: DataFrame of Orders with Full Name of the Contact")
    df_2 = get_orders_with_contact_address(orders_df)
    df_2.show(df_2.count(), truncate=False)  # Display the results