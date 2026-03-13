# Databricks notebook source
from datateam_moss.logger import get_logger
from datateam_moss import spark_io_utils as siu

from databricks.sdk.runtime import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.types import *
from typing import *
from datetime import datetime
import re


logger = get_logger("__cleaning__")

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

def to_databricks_boolean_column(column_name: str) -> Column:
    """
    Converteert boolean-achtige waarden in een PySpark kolom naar een echte boolean.
    Bekende representaties van booleans gemapt naar True of False.
    De resulterende kolom wordt gecast naar het Spark datatype `boolean`.

    Parameters
    ----------
    column_name : str
        De naam van de kolom die geconverteerd moet worden.

    Returns
    -------
    Column
        Een PySpark Column expressie waarbij de waarden zijn omgezet naar
        True, False of None met datatype `boolean`.

    Voorbeeld
    --------
    df = df.withColumn(
        "is_actief",
        to_databricks_boolean_column("is_actief")
    )
    """

    true_values = ["ja", "yes", "j", "y", "true", "t", "1"]
    false_values = ["nee", "no", "n", "false", "f", "0"]

    normalized = F.lower(F.trim(F.col(column_name).cast("string")))

    return (
        F.when(normalized.isin(true_values), True)
        .when(normalized.isin(false_values), False)
        .otherwise(None)
        .cast("boolean")
    )


def _parse_spark_type(type_str: str) -> DataType:
    """
    Deze functie parset datatypes om te bepalen of er een precision of scale in zit. 
    Dit is noodzakelijk zodat de json datatype DecimalType() wordt omgezet naar default precision namelijk 10,0,
    terwijl DecimalType(10,2) wordt behouden met de juiste precision en scale.

    Args:
        type_str (str):
            Het datatype dat geparsed dient te worden.

    Returns:
        DataType: Pyspark datatype

    Raises:
        KeyError:
            Indien een kolomtype niet wordt gevonden in de interne mapping van
            ondersteunde typen.
    """

    type_mapping = {
        "StringType": StringType,
        "LongType": LongType,
        "BooleanType": BooleanType,
        "TimestampType": TimestampType,
        "DoubleType": DoubleType,
        "IntegerType": IntegerType,
        "DecimalType": DecimalType,
        "DateType": DateType
    }

    #Geeft een match als een type is gevonden, anders lege string
    match = re.match(r"(\w+)\((.*?)\)", type_str)
    if not match:
        raise KeyError(f"Invalid type format '{type_str}'")

    #type_name is alles voor de haakjes, args alles erna dus "10,2"
    type_name, args = match.groups()

    if type_name not in type_mapping:
        raise KeyError(f"Unknown type '{type_name}'")

    if args.strip() == "":
        return type_mapping[type_name]()
    else:
        parsed_args = [int(a.strip()) for a in args.split(",")]
        return type_mapping[type_name](*parsed_args)


def cast_columns_from_schema(df: DataFrame, table_schema: Dict[str, Any]) -> DataFrame:
    """
    Cast DataFrame-kolommen op basis van een aangepast JSON-schema.

    Deze functie gaat ervan uit dat alle DataFrame-kolommen in eerste instantie als strings
    zijn ingelezen. Vervolgens wordt elke kolom gecast naar het type dat in het schema is
    gedefinieerd.

    Args:
        df (DataFrame):
            Het input-DataFrame waarin alle kolommen als string worden verwacht.
        table_schema (Dict[str, Any]):
            Een dictionary met de sleutel "columns", die een lijst bevat
            met kolomdefinities. Elke definitie moet bevatten:
                - "name": str  → de kolomnaam in het DataFrame
                - "type": str  → het Spark-type als string (bijv. "LongType()")
                - "nullable": bool

    Returns:
        DataFrame:
            Een nieuw DataFrame waarin alle kolommen zijn gecast naar hun bedoelde
            types.

        Voorbeeld:
            schema = {
                "columns": [
                    {"name": "ID", "type": "LongType()", "nullable": True},
                    {"name": "changedDate", "type": "TimestampType()", "nullable": True}
                ]
            }
            
            df_casted = cast_columns_from_schema(df=df, table_schema=table_schema)
    """

    for column in table_schema["columns"]:
        target_name: str = column.get("name", None)
        type_str: str = column.get("type", None)

        target_type: DataType = _parse_spark_type(type_str)

        df = df.withColumn(
            target_name,
            F.col(target_name).cast(target_type)
        )
    
    return df

def cleanse_and_prep_dataframe(df: DataFrame, table_schema: Dict, m_columns: List[str], runtime: datetime, rename_columns:List = None ) -> DataFrame:
    """
    Leest data uit de bron tabel, plakt de extra records onderaan en schrijft deze weg naar de doeltabel.

    Parameters:
    - df: De Dataframe die opgeschoont dient te worden.
    - table_schema: Het schema van de tabel.
    - m_columns: Een lijst met metadata kolommen.
    - runtime: De runtime van de notebook. 
    - rename_columns: Een dictionary met kolomnamen die moeten worden hernoemd.

    Returns:
    - None
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df moet een PySpark DataFrame zijn.")

    if not isinstance(table_schema, dict):
        raise TypeError("table_schema moet een dictionary zijn.")

    if "columns" not in table_schema:
        raise ValueError("table_schema moet een 'columns' key bevatten.")


    # Stap 1: drop de bronze metadata kolommen
    df = df.drop( "m_aangemaakt_op", "m_bijgewerkt_op", "m_runid")

    # Stap 2: Hernoem kolommen
    if rename_columns:
        for column in rename_columns:
            source_name = column.get("source_name")
            target_name = column.get("target_name")

            if source_name not in df.columns:
                logger.warning(f"Kolom {source_name} bestaat niet en wordt niet hernoemd.")
                continue
            
            df = df.withColumnRenamed(source_name, target_name)

    # Stap 3: format boolean indien nodig
    # Bepaal alle boolean kolommen uit het schema van de target tabel
    
    boolean_columns = [
    col["name"]
    for col in table_schema.get("columns", [])
    if col["type"] == "BooleanType()"
    ]
    
    # Cast naar boolean
    for column in boolean_columns:
        if column in df.columns:
            df = df.withColumn(column, to_databricks_boolean_column(column))

    # Stap 4: format date kolommen indien nodig

    date_columns = [
    col["name"]
    for col in table_schema.get("columns", [])
    if col["type"] == "DateType()"
    ]
    
    # Cast naar date
    for column in date_columns:
        if column in df.columns:
            parse_and_format_date(df=df,date_column=column)

    # Stap 4: voeg metadata kolommen toe
    df = siu.add_metadata_columns_to_dataframe(df=df, m_columns=m_columns, runtime=runtime, bron="JVS") #TODO: bron moet weg

    df = cast_columns_from_schema(df=df, table_schema=table_schema)
        
    return df