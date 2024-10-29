# Databricks notebook source

# Import necessary Databricks runtime objects
from databricks.sdk.runtime import *
from pyspark.sql import DataFrame, SparkSession

import pandas as pd

def bronanalyse(spark: SparkSession, catalog: str, schemas: list, trefwoorden_kolomnamen: list, counts = False, waarden = False):
    """
    Labelt de rijen van een DataFrame op basis van categorieën en matchende strings.

    Args:
        catalog (str): De naam van de catalog waar je de analyse op wil doen
        schemas (list): De namen van een of meerdere schema's waar je de analyse op wil doen
        trefwoorden_kolomnamen (list): Een of meerdere trefwoorden om kolomnamen op te selecteren
        counts (bool): De optie om een count en count distinct per kolom op te halen.
        waarden (bool): De optie om de eerste 5 unieke waarden per kolom op te halen.

    Returns:
        pd.Dataframe: een dataframe met tabel- en kolomnamen en bijbehorende counts en waarden.
    """
    
    #selectie van kolommen uit information_schema die we gebruiken voor bronanalyse
    kolommen_df = ['table_catalog','table_schema','table_name','column_name','data_type']

    #haal information schema op 
    df = spark.table(f'{catalog}.information_schema.columns').select(*kolommen_df)

    #filter information_schema tot relevante scheman
    schemas = '|'.join(schemas)
    df = df.where(df['table_schema'].contains(schemas))
                     
    #filter information_schema tot relevante kolomnamen
    kolomnamen = '|'.join(trefwoorden_kolomnamen)
    df = df.where(df['column_name'].contains(kolomnamen))

    #naar pandas
    df = df.toPandas()

    # bereken counts indien true
    if counts == True:
        #maak queries
        #count
        df['query_count'] = 'SELECT COUNT('+df['column_name']+') FROM '+df['table_catalog']+'.'+df['table_schema']+'.'+df['table_name']
        #count distinct
        df['query_count_distinct'] = 'SELECT COUNT(DISTINCT('+df['column_name']+')) FROM '+df['table_catalog']+'.'+df['table_schema']+'.'+df['table_name']
        #query data
        df['count'] = df['query_count'].apply(lambda x: spark.sql(x).first()[0])
        df['count_distinct'] = df['query_count_distinct'].apply(lambda x: spark.sql(x).first()[0])
    
    if waarden == True:
        #maak query
        df['query_waarden'] = 'SELECT DISTINCT('+df['column_name']+') FROM '+df['table_catalog']+'.'+df['table_schema']+'.'+df['table_name']+' LIMIT 5'
        #query data
        df['5_eerste_unieke_waarden'] = df['query_waarden'].apply(lambda x: ';'.join ([str(row[0]) for row in spark.sql(x).collect()]))
    
    #schoon dataframe op door queries te verwijderen
    if counts == True or waarden == True:
        for column in df.columns:
            if 'query' in column:
                df = df.drop(column,axis=1)
            else:
                pass
    return df
