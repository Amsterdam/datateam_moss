# Databricks notebook source
from databricks.sdk.runtime import *
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
import re

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

def parse_and_format_date(df, date_column, output_format="yyyy-MM-dd"):
    """
    Parseert en formatteert een kolom met datumstrings naar een uniform formaat zonder UDF's te gebruiken.

    Deze functie vervangt maandnamen door numerieke waarden, voegt voorloopnullen toe aan enkelvoudige 
    dag- en maandwaarden, en converteert vervolgens de genormaliseerde datum naar een gespecificeerd formaat.

    Args:
        df (pyspark.sql.DataFrame): Het invoer-DataFrame met de datumkolom.
        date_column (str): De naam van de kolom met datumstrings die moeten worden geparseerd.
        output_format (str, optioneel): Het gewenste uitvoerformaat, standaard is 'yyyy-MM-dd'.

    Returns:
        pyspark.sql.DataFrame: Het DataFrame met de bijgewerkte en geformatteerde datumkolom.
    """
    
    # Maandnamen omzetten naar numerieke waarden (Nederlands)
    month_map = {
        "jan": "01", "feb": "02", "mrt": "03", "apr": "04",
        "mei": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "okt": "10", "nov": "11", "dec": "12"
    }
    
    # Vervang maandnamen (bijvoorbeeld 'jan' naar '01')
    for month_name, month_num in month_map.items():
        df = df.withColumn(
            date_column,
            F.regexp_replace(
                F.col(date_column),
                rf"(?i)-{month_name}-",  # (?i) -> case-insensitive matching voor maandnamen
                f"-{month_num}-"         # Vervangt maandnaam door nummer
            )
        )
    
    # Voeg voorloopnullen toe aan enkelvoudige dag- en maandwaarden (bijvoorbeeld '1-1-1980' naar '01-01-1980')
    df = df.withColumn(
        "normalized_date",
        F.regexp_replace(F.col(date_column), r"\b(\d{1})\b", r"0\1")
    )

    # Mogelijke datumformaten die kunnen worden herkend
    possible_date_formats = [
        "dd-MM-yyyy HH:mm:ss",  # Formaat: 01-01-1980 00:00:00
        "dd-MM-yyyy",           # Formaat: 01-01-1980
        "yyyy-MM-dd",           # Formaat: 1980-01-01
        "MM/dd/yyyy",           # Formaat: 01/01/1980
        "dd-MMM-yyyy HH:mm:ss", # Formaat: 01-jan-1980 00:00:00
        "dd-MMM-yyyy"           # Formaat: 01-jan-1980
    ]
    
    # Probeer de genormaliseerde datumkolom te parsen met elk formaat
    parsed_column = None
    for fmt in possible_date_formats:
        current_parsed = F.to_date(F.col("normalized_date"), fmt)
        parsed_column = current_parsed if parsed_column is None else F.coalesce(parsed_column, current_parsed)
    
    # Voeg een kolom toe met de geparseerde datum
    df = df.withColumn("parsed_date", parsed_column)
    
    # Formatteer de geparseerde datum naar het gewenste uitvoerformaat
    df = df.withColumn(
        date_column, 
        F.date_format(F.col("parsed_date"), output_format).cast("date")
    )
    
    # Verwijder tussenliggende kolommen en retourneer het resultaat
    return df.drop("parsed_date", "normalized_date")
