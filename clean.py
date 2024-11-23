import logging
from fuzzywuzzy import fuzz
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

threshold: int = 80
company_names_file = 'company_names.csv'

def clean_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Dynamically cluster similar company names and standardize them.
    Clean and standardize company names dynamically using fuzzy matching.
    TODO: This should be a process more deterministic. 
    The ideal scenario would be providing a known names list to fuzzywuzzy to be more accurate

    :param df: PySpark DataFrame containing data.
    :param column_name: The name of the column to be cleaned.
    :return: Dataframe with standardized names called `materialization_[column_name]`
    """

    # Extract unique elements in DataFrame
    unique_names = [row[column_name] for row in df.select(column_name).distinct().collect()]
    logging.debug(f"Unique names extracted from column '{column_name}': {unique_names}")

    # Cluster similar names
    clusters = {}
    for name in unique_names:
        found_cluster = False
        for cluster_name in clusters.keys():
            if fuzz.ratio(name, cluster_name) >= threshold:  # Compare to cluster representative
                clusters[cluster_name].append(name)
                found_cluster = True
                break
        if not found_cluster:
            clusters[name] = [name]  # Create new cluster
    logging.debug(f"Clusters formed: {clusters}")

    # Map all names to their cluster representative (e.g., first name in each cluster)
    mapping = {name: representative for representative, names in clusters.items() for name in names}
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