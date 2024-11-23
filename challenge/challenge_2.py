import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, concat_ws, explode

logging.getLogger(__name__)

def get_orders_with_full_name(orders_df: DataFrame) -> DataFrame:
    """
    Create a DataFrame with order_id and contact_full_name.
    If contact name or surname is missing, use "John Doe" as a placeholder.

    Assumption: A full name withouth either first name or last name is not valid

    :param orders_df: PySpark DataFrame containing orders data.
    :return: PySpark DataFrame with columns order_id and contact_full_name.
    """
    try:
        logging.info("Creating DataFrame with contact full name.")

        # Extract contact_name and contact_surname from the parsed JSON
        orders_with_contact = orders_df.withColumn("contact", explode("contact_array")) \
                                             .withColumn("contact_name", col("contact.contact_name")) \
                                             .withColumn("contact_surname", col("contact.contact_surname"))

        # Concatenate full name only when both contact_name and contact_surname are not null or empty
        result = orders_with_contact.withColumn(
            "contact_full_name",
            when(
                (col("contact_name").isNotNull()) & (col("contact_name") != "") &
                (col("contact_surname").isNotNull()) & (col("contact_surname") != ""),
                concat_ws(" ", col("contact_name"), col("contact_surname"))
            ).otherwise(lit("John Doe"))
        ).select("order_id", "contact_full_name")

        logging.info("Successfully created DataFrame with contact full name.")
        return result
    except Exception as e:
        logging.error(f"Error in get_orders_with_full_name: {e}")
        raise


def challenge_2(orders_df: DataFrame):
    logging.info("Running Challenge 2: DataFrame of Orders with Full Name of the Contact")
    df_1 = get_orders_with_full_name(orders_df)
    df_1.show(df_1.count(), truncate=False)  # Display the results