# Databricks notebook source
import re
import pytz
from datetime import datetime
import pandas as pd
from databricks.sdk.runtime import *

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  
from pyspark.sql import types as T      
from pyspark.sql.window import Window   

def sla_tabel_op_catalog(df: DataFrame, catalog: str, schema: str, tabel_naam: str, operatie: str, keuze: str):
    """
    Sla een DataFrame op als een tabel in een Databricks catalogus met specifieke opties.

    Parameters:
    df (DataFrame): De DataFrame die moet worden opgeslagen.
    catalog (str): De catalogus waarin de tabel moet worden opgeslagen.
    schema (str): Het schema waarin de tabel moet worden opgeslagen.
    tabel_naam (str): De naam van de tabel die moet worden opgeslagen.
    operatie (str): De operatie die moet worden uitgevoerd ('mergeSchema' of 'overwriteSchema').
    keuze (str): De keuze voor de operatie ('true' of 'false').

    Returns:
    None
    """
    if operatie not in ["mergeSchema", "overwriteSchema"]:
        raise ValueError("Ongeldige operatie. Kies uit 'mergeSchema' of 'overwriteSchema'.")
    
    if keuze not in ["true", "false"]:
        raise ValueError("Ongeldige keuze. Kies uit 'true' of 'false'.")

    options = {operatie: keuze}

    if operatie == "mergeSchema" and keuze == "true":
        df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tabel_naam}")
    elif operatie == "mergeSchema" and keuze == "false":
        df.write.option("mergeSchema", "false").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tabel_naam}")
    elif operatie == "overwriteSchema" and keuze == "true":
        df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tabel_naam}")
    elif operatie == "overwriteSchema" and keuze == "false":
        df.write.option("overwriteSchema", "false").mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tabel_naam}")

def get_catalog():
    """
    Get the catalog for the current workspace.
    """
    return dbutils.secrets.get(scope='keyvault', key='catalog')

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
        set_tabellen_catalog_filter -= uitsluiten_set

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

def schrijf_naar_metatabel(catalogus: str, meta_schema: str, tabel_schema: str, tabel_naam: str, script_naam: str, controle: str, controle_waarde: str, meta_tabel_naam: str):
    """
    Schrijft gegevens naar de meta-tabel in de opgegeven catalogus en schema.

    Args:
        catalogus (str): De naam van de catalogus waarin de meta-tabel zich bevindt.
        meta_schema (str): De naam van het schema waarin de meta-tabel zich bevindt.
        tabel_schema (str): De naam van het schema van de tabel waarvoor de gegevens worden toegevoegd.
        tabel_naam (str): De naam van de tabel waarvoor de gegevens worden toegevoegd.
        script_naam (str): De naam van het script dat de gegevens toevoegt.
        controle (str): Informatie over de controle.
        controle_waarde (str): De waarde van de controle.
        meta_tabel_naam (str): De naam van de meta-tabel waar gegevens worden opgeslagen.

    Returns:
        None
    """
    try: 
        meta_tabel_df = spark.read.table(f"{catalogus}.{meta_schema}.{meta_tabel_naam}")
        data = [(catalogus, tabel_schema, tabel_naam, script_naam, controle, controle_waarde)]
        kolommen = ["table_catalog", "table_schema", "table_name", "script", "controle", "controle_waarde"]
        temp_tabel = spark.createDataFrame(data, kolommen)
        union_df = meta_tabel_df.union(temp_tabel)

        updated_df = (union_df
                    .select([F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) for c in union_df.columns])
                    .na.drop(how="all").distinct())

        updated_df.write.saveAsTable(f"{catalogus}.{meta_schema}.{meta_tabel_naam}", mode="overwrite")
    
    except:
        data = [(catalogus, tabel_schema, tabel_naam, script_naam, controle, controle_waarde)]
        kolommen = ["table_catalog", "table_schema", "table_name", "script", "controle", "controle_waarde"]
        temp_tabel = spark.createDataFrame(data, kolommen)
        temp_tabel.write.saveAsTable(f"{catalogus}.{meta_schema}.{meta_tabel_naam}", mode="overwrite")

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
    """
    converted_format = convert_datetime_format(tijdformaat)
    amsterdam_tz = pytz.timezone('Europe/Amsterdam')
    huidige_datum = datetime.now(amsterdam_tz).strftime(tijdformaat)

    if date_string_timestamp == "string":
        return huidige_datum
    elif date_string_timestamp == "timestamp":
        return F.to_timestamp(F.lit(huidige_datum), converted_format)
    elif date_string_timestamp == "date":
        return F.to_date(F.lit(huidige_datum), converted_format)

def vind_scheidingsteken(bestandspad, scheidingstekens=[',', ';', '\t', '|']):
    """
    Probeert het juiste scheidingsteken (delimiter) te vinden voor een CSV-bestand door verschillende mogelijke scheidingstekens te testen.

    Deze functie leest het CSV-bestand met elk gegeven scheidingsteken en controleert of het resulterende DataFrame
    meer dan één kolom bevat. Indien ja, wordt aangenomen dat het gevonden scheidingsteken correct is.

    Args:
        bestandspad (str): Het pad naar het CSV-bestand in the UC.
        scheidingstekens (list): Lijst van mogelijke scheidingstekens om te testen. Standaard zijn dit: [',', ';', '\t', '|'].
    """
    for scheidingsteken in scheidingstekens:
        try:
            df = spark.read.csv(bestandspad, sep=scheidingsteken, header=True)
            if len(df.columns) > 1:
                return scheidingsteken, df.columns
        except Exception:
            continue
    return None, None
