import logging
import jellyfish
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

threshold: float = 0.93  # Set a threshold for Jaro-Winkler similarity. Lower is risky
company_names_file = 'company_names.csv'

def clean_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Dynamically cluster similar company names and standardize them using Jaro-Winkler distance.
    Comparisons are case-insensitive, but the original case of names is preserved in the output.

    This algorithm to normalize names can make mistakes.
    TODO: Including a file with a trusted source to help to this algorithm to increase the accuracy

    :param df: PySpark DataFrame containing data.
    :param column_name: The name of the column to be cleaned.
    :return: DataFrame with standardized names called `materialization_[column_name]`.
    """

    # Extract unique elements in DataFrame
    unique_names = [row[column_name] for row in df.select(column_name).distinct().collect()]
    logging.debug(f"Unique names extracted from column '{column_name}': {unique_names}")

    # Create clusters with case-insensitive comparison
    clusters = {}
    for name in unique_names:
        normalized_name = name.lower()  # Normalize to lowercase for comparison
        found_cluster = False
        for cluster_name in clusters.keys():
            if jellyfish.jaro_winkler_similarity(normalized_name, cluster_name.lower()) >= threshold:
                clusters[cluster_name].append(name)
                found_cluster = True
                break
        if not found_cluster:
            clusters[name] = [name]  # Create a new cluster with the original name
    logging.debug(f"Clusters formed: {clusters}")

    # Map all names to their cluster representative (keeping original case)
    mapping = {}
    for representative, names in clusters.items():
        for name in names:
            mapping[name] = representative
    logging.debug(f"Mapping created: {mapping}")

    # Creating UDF to apply the mapping
    map_udf = udf(lambda name: mapping.get(name, name), StringType())

    # Replace the column with the cleaned info
    new_column = f"materialization_{column_name}"
    cleaned_df = df.withColumn(new_column, map_udf(col(column_name)))

    # Write the DataFrame to a CSV file, overwriting existing files
    cleaned_df.select(cleaned_df[column_name].alias("original_company_name"),
                      cleaned_df[new_column].alias('new_company_name')) \
                      .orderBy("original_company_name") \
                      .toPandas() \
                      .to_csv(company_names_file, index=False)

    return cleaned_df