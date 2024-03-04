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

def del_meerdere_tabellen_catalog(catalog: str, schema: str, tabellen_filter: str, uitsluiten_tabellen:str = None):
    """
    Verwijder meerdere tabellen uit de catalogus.

    Args:
        catalog (str): Naam van de catalogus.
        schema (str): Naam van het schema.
        tabellen_filter (str): Filter voor tabellen die moeten worden verwijderd.
        uitsluiten_tabellen (str, optional): Tabellen om uit te sluiten van verwijdering. Standaard is None.

    Raises:
        ValueError: Als er geen tabellen zijn die overeenkomen met het opgegeven filter.

    Returns:
        None
    """
    
    # Combineer catalogus en schema
    schema_catalog = catalog + "." + schema
    
    # Haal metadata op uit de Unity Catalog
    tabellen_catalog = spark.sql(f"SHOW TABLES IN {schema_catalog}")

    # Maak een set van alle tabellen in het opgegeven schema
    set_tabellen_catalog = {row["tableName"] for row in tabellen_catalog.collect()}
    set_tabellen_catalog_filter = {row for row in set_tabellen_catalog if tabellen_filter in row}

    # Controleer of er tabellen zijn die voldoen aan het opgegeven filter
    if len(set_tabellen_catalog_filter) == 0:
        raise ValueError("Er bestaan geen tabellen met opgegeven tabellen_filter. Vul de parameter uitsluiten_tabellen aan of geef een ander filter op.")
    
    # Print de geselecteerde tabellen
    print(set_tabellen_catalog_filter)
    
    # Vraag om bevestiging voor verwijdering van de tabellen
    verwijder_check = input("Je staat op het punt om deze tabellen te verwijderen uit de CATALOG. Typ 'ja' om de tabellen te verwijderen.")
    
    # Verwijder de geselecteerde tabellen indien bevestigd
    if verwijder_check == "ja":
        for table in set_tabellen_catalog_filter:
            spark.sql(f"DROP TABLE {catalog}.{schema}.{table}")
            print("De opgegeven tabellen zijn correct verwijderd.") 

    return
    
