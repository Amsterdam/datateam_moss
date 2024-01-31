import re
from databricks.sdk.runtime import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.window import Window

def clean_column_names(cols):
    """
    Clean and standardize column names by converting to lowercase, replacing unwanted characters with underscores,
    collapsing consecutive underscores, and removing leading/trailing whitespaces and underscores.

    Args:
        cols (list): List of column names to be cleaned.

    Returns:
        list: List of cleaned column names.

    Example:
        test_strings = [" First Name ", "_Last_Name_", "__Email-Address__", "Liquiditeit (current ratio)", "fte's mannen 2021?", "2017/'18*"]
        clean_column_names(test_strings)

    Usage:
        # spark
        df = df.toDF(*clean_column_names(df.columns))

        # polars
        df.columns = clean_column_names(df.columns)
    """
    # Unwanted
    UNWANTED_CHARS = r"[ ,;{}:()\n\t=\.\-/'?\*]"
    cleaned_cols = [re.sub(UNWANTED_CHARS, '_', col.lower()).replace('___', '_').replace('__', '_').strip('_').strip() for col in cols]
    return cleaned_cols


def rename_multiple_columns(df: DataFrame, renamings: dict):
    """
    Renames multiple columns in a PySpark DataFrame.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame.
        renamings (dict): A dictionary containing the column name mappings.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with renamed columns.
    """
    return df.toDF(*list(map(lambda x: renamings.get(x, x), df.columns)))
