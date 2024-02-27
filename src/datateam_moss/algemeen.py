# Databricks notebook source
import re
from datetime import datetime

from databricks.sdk.runtime import *
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def get_catalog():
    """
    Get the catalog for the current workspace.
    """
    return dbutils.secrets.get(scope='keyvault', key='catalog')


def clean_column_names(cols):
    """
    Clean and standardize column names by converting to lowercase, replacing unwanted characters with underscores,
    collapsing consecutive underscores, and removing leading/trailing whitespaces and underscores.

    Otherwise, spark will throw this error on Unity Catalog:
        `Found invalid character(s) among ' ,;{}()\n\t=' in the column names of your schema.`

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
    

def clean_dataframe(df: DataFrame):
    """
    Clean and standardize dataframe column names by converting to lowercase, replacing unwanted characters with underscores,
    collapsing consecutive underscores, and removing leading/trailing whitespaces and underscores.

    Otherwise, spark will throw this error on Unity Catalog:
        `Found invalid character(s) among ' ,;{}()\n\t=' in the column names of your schema.`

    Args:
        df (pyspark.sql.DataFrame): DataFrame to be cleaned.

    Returns:
        pyspark.sql.DataFrame: Cleaned DataFrame.
    """
    return df.toDF(*clean_column_names(df.columns))
    