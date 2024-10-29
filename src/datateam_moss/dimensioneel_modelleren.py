# Databricks notebook source

# Import algemene packagaes
import sys
import time
import uuid
import hashlib
import xxhash
import random
import string

# Import packages voor pyspark
from databricks.sdk.runtime import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.window import Window

def voeg_willekeurig_toe_en_hash_toe(df: DataFrame, business_key: str, naam_id: str):
    """
    Neemt de naam van een kolom als invoer aan, veronderstelt dat het een gehashte waarde bevat,
    voegt aan elke waarde een willekeurig woord of getal toe en maakt een nieuwe hash..

    Parameters:
    - df: DataFrame: Het DataFrame waar je de hash aan wil toevoegen
    - business_key: str: De naam van de invoerkolom.
    - naam_id: str: De naam van de primary key

    Returns:
    - DataFrame: Een DataFrame met de oorspronkelijke gehashte waarde en de nieuwe gehashte waarde.
    """
    def voeg_willekeurig_toe_en_hash_toe_udf(waarde):
        """
        Neemt een waarde, voegt er een willekeurig woord of getal aan toe en maakt een nieuwe hash.

        Parameters:
        - waarde: De invoerwaarde.

        Returns:
        - int: De nieuwe gehashte waarde als integer.
        """
        willekeurig_deel = ''.join(random.choices(string.ascii_letters + string.digits, k=10))

        # Controleer het type van de invoerwaarde en handel elk geval dienovereenkomstig af
        if isinstance(waarde, int):
            waarde_str = str(waarde)
        elif isinstance(waarde, bool):
            waarde_str = str(waarde)
        else:
            waarde_str = str(waarde)

        geconcateneerde_reeks = waarde_str + willekeurig_deel
        nieuwe_hash_waarde = xxhash.xxh64(geconcateneerde_reeks).intdigest()

        # Modulo operation to limit the hash value within the range of LongType
        hash_in_range = nieuwe_hash_waarde % (2**63 - 1)  # Maximum value for LongType
        return hash_in_range

    # Registreer de UDF
    udf_voeg_willekeurig_toe_en_hash_toe = udf(voeg_willekeurig_toe_en_hash_toe_udf, returnType=LongType())
    
    # Pas de UDF toe op het DataFrame
    resultaat_df = (df.withColumn(naam_id, udf_voeg_willekeurig_toe_en_hash_toe(col(business_key))))
    return resultaat_df


def maak_onbekende_dimensie(df, naam_bk, naam_id="", uitzonderings_kolommen=[]):
    """
    Maakt een nieuwe DataFrame met een record voor ontbrekende waarden in een dimensietabel.

    Args:
        df (DataFrame): Het bron DataFrame.
        naam_bk (str): De naam van de business_key (BK) kolom.
        naam_id (str): De naam van de primaire sleutel / ID kolom.
        uitzonderings_kolommen (list, optional): Lijst van kolommen die moeten worden uitgesloten bij het vergelijken van versies. Standaard is leeg.

    Returns:
        DataFrame: Een nieuwe DataFrame met een record voor ontbrekende waarden.

    Raises:
        ValueError: Als de opgegeven kolomnaam voor de ID niet aanwezig is in het DataFrame.
    """
    nieuwe_data = []

    for col in df.columns:
        col_type = df.schema[col].dataType
        if col == naam_bk:
            nieuwe_data.append("0")
        elif col == naam_id:
            nieuwe_data.append(0)
        elif isinstance(col_type, DoubleType):
            nieuwe_data.append(0.00)
        elif isinstance(col_type, IntegerType) or isinstance(col_type, LongType):
            nieuwe_data.append(0)
        elif isinstance(col_type, StringType):
            nieuwe_data.append(f'N/A')
        elif isinstance(col_type, BooleanType):
            nieuwe_data.append(None)
        elif isinstance(col_type, TimestampType):
            datum_tijd = datetime.strptime("9999-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
            nieuwe_data.append(datum_tijd)
        elif isinstance(col_type, DateType):
            datum_tijd = datetime.strptime("9999-12-31", "%Y-%m-%d").date()
            nieuwe_data.append(datum_tijd)
        elif isinstance(col_type, NullType):
            nieuwe_data.append('N/A') 

    nieuwe_rij = Row(*nieuwe_data)
    schema = StructType([StructField(col, df.schema[col].dataType, True) for col in df.columns])

    # Bepaal het schema op basis van het dataframe
    onbekende_dimensie = spark.createDataFrame([nieuwe_rij], schema)  # Maak een nieuwe rij
    df_met_onbekende_dimensie = onbekende_dimensie.union(df)
    return df_met_onbekende_dimensie


def vul_lege_cellen_in(df: DataFrame, uitzonderings_kolommen: list = []):
    """
    Vult lege cellen in een DataFrame in met standaardwaarden, behalve voor de opgegeven uitzonderingskolommen.

    Args:
        df (DataFrame): Het DataFrame dat moet worden bewerkt.
        uitzonderings_kolommen (list, optional): Lijst van kolommen waarvoor lege cellen niet moeten worden ingevuld. Standaard is leeg.

    Returns:
        DataFrame: Het DataFrame met lege cellen ingevuld met standaardwaarden, behalve voor de uitzonderingskolommen.
    """
    for col_name in df.drop(*uitzonderings_kolommen).columns:
        col_type = df.schema[col_name].dataType
        if isinstance(col_type, DoubleType):
            df = (df.withColumn(col_name, when(column(col_name).isNull(), lit(0.00)).otherwise(column(col_name))))
        elif isinstance(col_type, IntegerType) or isinstance(col_type, LongType):
            df = (df.withColumn(col_name, when(column(col_name).isNull(), lit(0)).otherwise(column(col_name))))
        elif isinstance(col_type, StringType):
            df = (df.withColumn(col_name, when(column(col_name).isNull(), lit('N/A')).otherwise(column(col_name))))
        elif isinstance(col_type, BooleanType):
            df = (df.withColumn(col_name, when(column(col_name).isNull(), lit(None)).otherwise(column(col_name))))
        elif isinstance(col_type, TimestampType):
            datum_tijd = datetime.strptime("9999-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")
            df = (df.withColumn(col_name, when(column(col_name).isNull(), lit(datum_tijd)).otherwise(column(col_name))))
        elif isinstance(col_type, DateType):
            datum_tijd = datetime.strptime("9999-12-31", "%Y-%m-%d").date()
            df = (df.withColumn(col_name, when(column(col_name).isNull(), lit(datum_tijd)).otherwise(column(col_name))))
    return df


def prepareer_dimensie_tabel(df: DataFrame, naam_bk: str, naam_id: str, uitzonderings_kolommen=[]):
    """
    Bereidt een dimensietabel voor door verschillende transformaties toe te passen.
    Deze functie voegt een unieke sleutel toe aan de tabel, voegt een "onbekende" dimensie toe, en vult lege cellen in de dimensietabel in.

    Args:
        df (DataFrame): Het DataFrame van de dimensietabel.
        naam_bk (str): Naam van de business key (kolom) in de tabel.
        naam_id (str): Naam van de ID-kolom in de tabel.
        uitzonderings_kolommen (list, optional): Lijst van kolommen waarvan de lege cellen niet ingevuld moeten worden. Standaard is leeg.

    Returns:
        DataFrame: Het voorbereide dimensietabel DataFrame na het toepassen van transformaties.
    """
    
    # Voeg unieke sleutel toe aan de tabel
    df_hashed = voeg_willekeurig_toe_en_hash_toe(df=df, business_key=naam_bk, naam_id=naam_id)

    # Voeg een "onbekende" dimensie toe aan de tabel
    df_hashed_onbekend = maak_onbekende_dimensie(df=df_hashed, naam_bk=naam_bk, naam_id=naam_id, uitzonderings_kolommen=[])

    # Vul de lege cellen in de dimensietabel in
    df_hashed_onbekend_ingevuld_lege_cellen = vul_lege_cellen_in(df=df_hashed_onbekend, uitzonderings_kolommen=[])

    return df_hashed_onbekend_ingevuld_lege_cellen
