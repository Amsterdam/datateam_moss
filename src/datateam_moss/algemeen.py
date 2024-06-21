# Databricks notebook source
import re
import pytz
from datetime import datetime
import pandas as pd

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

def del_meerdere_tabellen_catalog(catalog: str, schema: str, tabellen_filter: str, uitsluiten_tabellen: str = None):
    """
    Verwijder meerdere tabellen uit de catalogus.

    Args:
        catalog (str): Naam van de catalogus.
        schema (str): Naam van het schema.
        tabellen_filter (str): Filter voor tabellen die moeten worden verwijderd: "alles" of "prefix in tabelnaam"
        uitsluiten_tabellen (str, optional): Tabellen om uit te sluiten van verwijdering. Standaard is None.

    Raises:
        ValueError: Als er geen tabellen zijn die overeenkomen met het opgegeven filter.

    Returns:
        None
    """

    # Combineer catalogus en schema
    schema_catalog = f"{catalog}.{schema}"
    
    # Haal metadata op uit de Unity Catalog
    tabellen_catalog = spark.sql(f"SHOW TABLES IN {schema_catalog}")

    # Maak een set van alle tabellen in het opgegeven schema
    set_tabellen_catalog = {row["tableName"] for row in tabellen_catalog.collect()}

    # Filter de tabellen op basis van de gegeven filter
    if tabellen_filter == "alles":
        set_tabellen_catalog_filter = set_tabellen_catalog
    else:
        set_tabellen_catalog_filter = {table for table in set_tabellen_catalog if tabellen_filter in table}

    # Verwijder de tabellen die in uitsluiten_tabellen staan, als deze parameter is meegegeven
    if uitsluiten_tabellen:
        uitsluiten_set = set(uitsluiten_tabellen.split(","))
        set_tabellen_catalog_filter = set_tabellen_catalog_filter - uitsluiten_set

    # Controleer of er tabellen zijn die voldoen aan het opgegeven filter
    if not set_tabellen_catalog_filter:
        raise ValueError("Er bestaan geen tabellen met het opgegeven tabellen_filter. Vul de parameter uitsluiten_tabellen aan of geef een ander filter op.")
    
    # Print de geselecteerde tabellen
    print("Geselecteerde tabellen voor verwijdering:", set_tabellen_catalog_filter)
    
    # Vraag om bevestiging voor verwijdering van de tabellen
    verwijder_check = input("Je staat op het punt om deze tabellen te verwijderen uit de CATALOG. Typ 'ja' om de tabellen te verwijderen -> ")
    
    # Verwijder de geselecteerde tabellen indien bevestigd
    if verwijder_check.lower() == "ja":
        for table in set_tabellen_catalog_filter:
            spark.sql(f"DROP TABLE {schema_catalog}.{table}")
        print("De opgegeven tabellen zijn correct verwijderd.") 
    else:
        print("Verwijdering geannuleerd.")
    return

def check_nrow_tabel_vs_distinct_id(tabelnaam: str, id: str):
    """
    Controleert of het aantal rijen overeenkomt met het aantal unieke ID's in de opgegeven kolom.

    Args:
        tabelnaam (str): Naam van de tabel.
        id (str): Naam van de kolom die de unieke ID's bevat.

    Raises:
        ValueError: Als het aantal unieke ID's niet overeenkomt met het totale aantal rijen in de tabel.

    Returns:
        None
    """
    
    # Lees de tabel in
    check_tabel = spark.read.table(tabelnaam)
    
    # Bereken het aantal unieke ID's
    distinct_count = check_tabel.select(id).distinct().count()
    
    # Bereken het totale aantal rijen
    total_count = check_tabel.count()

    # Controleer of het aantal unieke ID's overeenkomt met het totale aantal rijen
    if distinct_count == total_count:
        print("Check succes: Het aantal rijen komt overeen met het aantal unieke ID's")
    else:
        raise ValueError("Check gefaald: Het aantal rijen komt NIET overeen met het aantal unieke ID's")
    
    return

def controle_unieke_waarden_kolom(df: DataFrame, kolom: str):
    """
    Controleert of alle waarden in een specifieke kolom uniek zijn in het gegeven DataFrame.

    Parameters:
    - df: DataFrame: Het DataFrame waarin de controle wordt uitgevoerd.
    - kolom: str: De naam van de kolom waarvan de unieke waarden worden gecontroleerd.

    Returns:
    - None

    Error:
    - ValueError: Als het aantal unieke waarden in de kolom niet gelijk is aan het totale aantal rijen,
                   wordt er een melding geprint dat niet alle waarden in de kolom uniek zijn.
    
    Laatste update: 10-01-2023
    """
    window_spec = Window().partitionBy(kolom)
    df_with_counts = (
        df.join(broadcast(df.dropDuplicates([kolom])), kolom, "inner")
        .select(kolom, count(kolom).over(window_spec).alias("count"))
        .filter(col("count") > 1)
        .distinct()
    )
    
    # If-statement om te controleren of er dubbele business_keys zijn
    if (df_with_counts.isEmpty()):
        print(f"Er zijn geen dubbele waarden gedetecteerd in de opgegeven kolom ({kolom}) van de tabel.")  
    else:
        raise ValueError(f"Niet alle waarden in de kolom '{kolom}' zijn uniek.")
    return

def schrijf_naar_metatabel(table_catalog:str, table_schema:str, table_naam:str, script: str, controle: str, controle_waarde: str, meta_tabel: str):
    """
    Schrijft gegevens naar de meta-tabel in de opgegeven catalogus en schematabel.

    Laadt de bestaande meta-tabel in, voegt nieuwe gegevens toe, vervangt lege strings door None,
    verwijdert rijen die volledig leeg zijn en schrijft de bijgewerkte gegevens terug naar de meta-tabel.

    !! Let op !! Als je eerste keer deze functie gebruikt moet je even lege waardes meegeven.
    
    Args:
        script (str): De naam van het script.
        tabel (str): De naam van de tabel.
        controle (str): De controle informatie.
        controle_waarde (str): De waarde van de controle.
        meta_tabel (str): De naam van de meta-tabel waar gegevens worden opgeslagen.

    Returns:
        None
    """    
    try: 
    # Laad de meta-tabel in
        meta_tabel_df = spark.read.table(f"{table_catalog}.algemeen.{meta_tabel}")

        # Definieer het schema & data
        data = [(table_catalog, table_schema, table_naam, script, controle, controle_waarde)]
        schema = ["table_catalog", "table_schema", "table_name", "script", "controle", "controle_waarde"]

        # Creëer een tijdelijke DataFrame met de nieuwe gegevens
        temp_tabel = spark.createDataFrame(data, schema)

        # Voeg de tijdelijke DataFrame samen met de bestaande meta-tabel
        union_df = meta_tabel_df.union(temp_tabel)

        # Vervang lege strings door None en verwijder volledig lege rijen
        updated_df = (union_df
                    .select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in union_df.columns])
                    .na.drop(how="all").distinct())

        # Schrijf de bijgewerkte DataFrame terug naar de meta-tabel
        updated_df.write.saveAsTable(f"{table_catalog}.algemeen.{meta_tabel}", mode="overwrite")
    
    except:
        # Definieer het schema & data
        data = [(table_catalog, table_schema, table_naam, script, controle, controle_waarde)]
        schema = ["table_catalog", "table_schema", "table_name", "script", "controle", "controle_waarde"]

        # Creëer een tijdelijke DataFrame met de nieuwe gegevens
        temp_tabel = spark.createDataFrame(data, schema)

        # Schrijf de bijgewerkte DataFrame terug naar de meta-tabel
        temp_tabel.write.saveAsTable(f"{table_catalog}.algemeen.{meta_tabel}", mode="overwrite")
    return

def convert_datetime_format(input_format):
    """
    Converteert het opmaakoptie voor datum en tijd van het datetime (format)  naar het PySpark format.
    
    Args:
        input_format (str): De invoeropmaakoptie voor datum en tijd.
    
    Returns:
        str: De geconverteerde opmaakoptie voor datum en tijd.
    """
    # Mapt de formatting van de package: datetime -> PySpark formatting
    format_mapping = {
        "%Y": "yyyy",
        "%m": "MM",
        "%d": "dd",
        "%H": "HH",
        "%M": "mm",
        "%S": "ss"
    }

    # Vervang opmaaktekens in de opgegeven string
    for char, replacement in format_mapping.items():
        input_format = input_format.replace(char, replacement)
    
    return(input_format)


def tijdzone_amsterdam(tijdformaat="%Y-%m-%d %H:%M:%S", date_string_timestamp="timestamp"):
    """Haalt de huidige tijd op en converteert deze naar het opgegeven tijdsformaat en de tijdzone van Amsterdam.
       Kijk op https://www.w3schools.com/python/python_datetime.asp voor de formatting
    Args:
        tijdformaat (str, optioneel): De opmaakstring voor het gewenste tijdsformaat. Standaard ingesteld op "%Y-%m-%d %H:%M:%S".
        date_string_timestamp (str, optioneel) = Hier kn je aangeven of je het terug wil als een "timestamp" (PySpark-kolom) of "date" (Pyspark-kolom) of "string" format.

    Returns:
        str of Timestamp: De huidige tijd in het opgegeven formaat en de tijdzone van Amsterdam.
    """
    # Converteer datetime opmaak naar PySpark opmaak
    converted_format = convert_datetime_format(tijdformaat)

    # Haal de huidige tijd op
    amsterdam_tz = pytz.timezone('Europe/Amsterdam')
    huidige_datum = datetime.now(amsterdam_tz).strftime(tijdformaat)

    # Als return_type 'string' is, geef de tijd als string terug
    if date_string_timestamp == "string":
        return huidige_datum
    
    # Als het gaat om jaar, maand of dag aanduiding -> dan dataformat
    elif date_string_timestamp == "timestamp":
        timestamp_expr = to_timestamp(lit(huidige_datum), converted_format)
        return timestamp_expr
        
    elif date_string_timestamp == "date":
        timestamp_expr = to_date(lit(huidige_datum), converted_format)
        return timestamp_expr


def bepaal_kolom_volgorde(df: DataFrame, gewenste_kolom_volgorde: list) -> DataFrame: 
    """
    Bepaalt de volgorde van kolommen in een DataFrame op basis van de opgegeven gewenste volgorde.

    Parameters:
    - df (DataFrame): Het invoer DataFrame waarvan de kolomvolgorde moet worden aangepast.
    - gewenste_kolom_volgorde (list): Een lijst met kolomnamen in de gewenste volgorde.

    Returns:
    - DataFrame: Een nieuw DataFrame met kolommen in de gespecificeerde volgorde.
    Laatste update: 22-02-2023
    """
    # Maak een kopie van de gewenste kolomvolgorde
    temp = gewenste_kolom_volgorde.copy()

    # Bepaal de juiste volgorde van de kolommen
    df_kolommen = df.columns
    for column in df_kolommen:
        if column not in temp:
            temp.append(column.lower())

    output_df = df.select(*temp)
    return output_df, temp.copy()  # Return the modified DataFrame and a copy of the modified column order
