# Databricks notebook source
from pyspark.sql import SparkSession, Row, DataFrame
from databricks.sdk.runtime import *

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
