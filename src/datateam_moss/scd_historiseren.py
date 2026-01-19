from pandas import DataFrame
import pyspark.sql.functions as F
from typing import Dict, Any
from delta.exceptions import ConcurrentWriteException
from delta.tables import DeltaTable
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from functools import reduce
spark = SparkSession.builder.getOrCreate()
import time
import re

#
# validate_merge_columns(

def validate_merge_columns(source_df: DataFrame, merge_condition: str) -> None:
    """
    Validates that the combination of all source columns used in the merge condition 
    is unique in the source DataFrame.
    
    Args:
        source_df (DataFrame): The source DataFrame.
        merge_condition (str): The merge condition string (e.g., 'target.id = source.id AND target.type = source.type').
    
    Raises:
        ValueError: If the combination of source columns is not unique in source_df.
    """
    # Extract all 'source.xxx' columns from merge_condition
    source_cols = re.findall(r'source\.([a-zA-Z0-9_]+)', merge_condition)
    
    if not source_cols:
        raise ValueError("No source columns found in the merge condition.")
    
    duplicate_count = (
        source_df.groupBy(source_cols)
        .count()
        .filter("count > 1")
        .count()
    )
    if duplicate_count > 0:
        raise ValueError(f"Validation failed: combination of columns {source_cols} is not unique in source_df.")

#
# restore_table_with_retry
#

def restore_table_with_retry(
    table_name: str,
    version: int,
    max_retries: int = 5,
    wait_seconds: int = 3
) -> None:
    """
    Attempts to restore a Delta table to a specified version with retry logic 
    in case of concurrent write conflicts.

    Args:
        table_name (str): The fully qualified name of the Delta table (e.g., 'catalog.schema.table').
        version (int): The version number to which the table should be restored.
        max_retries (int, optional): Maximum number of retry attempts. Defaults to 5.
        wait_seconds (int, optional): Number of seconds to wait between retries. Defaults to 3.

    Raises:
        Exception: If the restore fails after the maximum number of retries or if an unexpected error occurs.
    """
    retries = 0
    while retries < max_retries:
        try:
            spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
            return  # Success
        except ConcurrentWriteException:
            retries += 1
            time.sleep(wait_seconds)
        except Exception as e:
            raise Exception(f"❌ Unexpected error during restore: {e}") from e

    raise Exception(f"❌ Failed to restore table {table_name} after {max_retries} retries due to concurrent writes.")



#
# SCD2 merge versie 20260119
#

def perform_scd2_merge(
    source_df: DataFrame, 
    target_table_name: str,
    merge_condition: str,
    key_columns: Dict[str, str],
    bk_column: str,
    update_columns: Dict[str, str],
    insert_columns: Dict[str, str],
    m_runid: str,
    close_deleted_records: bool = False,
    source_validity_timestamp = None
    ) -> None:
    """Executes an SCD Type 2 merge operation using Delta Lake.

    - Updates existing records by closing the old record and inserting a new record with updated values.
    - Inserts new records with its values.
    - Optionally closes deleted records that are no longer present in the source.

    If the target table does not exist, it will be created based on the source_df. Note that the meta-columns m_geldig_van, m_geldig_tot and m_is_actief are added.

    Args:
        source_df (DataFrame): The source DataFrame containing new records.
        target_table_name (str): The name of the target Delta table (e.g., catalog.schema.table).
        merge_condition (str): SQL condition to match records.
        update_columns (Dict[str, str]): Columns to update when a match occurs.
        insert_columns (Dict[str, str]): Columns to insert when no match occurs.
        close_deleted_records (bool, optional): Whether to close old records when no match occurs in the source. Defaults to False.
    """

    # Validate that the combination of source merge columns is unique
    validate_merge_columns(source_df, merge_condition)

    M_RUNID = m_runid #F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
    # Validate that the combination of source merge columns is unique
    #validate_merge_columns(source_df, merge_condition)

    # indien de bron record timestamp als geldigheids van-tot wordt genomen wordt de timestamp vervangen door een source_validity_timestamp
    source_validity_timestamp = source_validity_timestamp if source_validity_timestamp is not None else F.current_timestamp()

   # Zorg dat bk_column een string is (haalt de waarde uit de lijst als dat nodig is)
    bk_column_name = bk_column[0] if isinstance(bk_column, list) else bk_column

    # maak business keys aan en voeg toe aan source_df
    cols_to_concat = [F.col(k).cast("string") for k in key_columns]

    if len(key_columns) == 1:
        # Als er maar één key kolom is, is de business key gelijk aan die kolom (ook omgezet naar string)
        #bk_column_value = F.col(key_columns[0]).cast("string")
        single_key_column_name = list(key_columns.keys())[0] 
        source_df_bk = source_df.withColumn(bk_column, F.col(single_key_column_name).cast("string"))
        # source_df = source_df.withColumn(bk_column_name, F.col(key_columns[0]).cast("string"))
    else:
        # Als er meerdere key kolommen zijn, concateneer ze met '~'
        # bk_column_value = F.concat_ws("~", *cols_to_concat)
        source_df_bk = source_df.withColumn(bk_column, F.concat_ws("~", *cols_to_concat))
    # source_df_bk.display()
    # source_df = source_df.withColumn(bk_column_name, bk_column_value)

    # If the target table does not exist, create it and fill it with data from source_df. Otherwise, perform merge operation.
    if not spark.catalog.tableExists(target_table_name):
        df_with_scd_fields = (source_df_bk
                            .withColumn("m_geldig_van", source_validity_timestamp)
                            .withColumn("m_geldig_tot", F.lit(None).cast("timestamp"))
                            .withColumn("m_bijgewerkt_op", F.lit(None).cast("timestamp"))
                            .withColumn("m_aangemaakt_op", F.current_timestamp())
                            .withColumn("m_is_actief", F.lit(True))
                            .withColumn("m_runid", F.lit(M_RUNID))           
                            )
        df_with_scd_fields.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(target_table_name)
    else:
        # Load the existing Delta table
        try:
            target_table = DeltaTable.forName(spark, target_table_name)
        except Exception as e:
            raise ValueError(f"Error loading Delta table '{target_table_name}': {e}")

        # Ensure we are only modifying active records
        _merge_condition = f"{merge_condition} AND m_is_actief = True"
        
        # Detect changes: If any update column differs in an active record, the record should be closed
        _update_condition = f"({' OR '.join([f'target.{col} IS DISTINCT FROM source.{col}' for col in update_columns.keys()])}) AND target.m_is_actief = True"
        
        ## Step 1: Close old records (update m_geldig_tot and m_is_actief)
        merge_step_1 = target_table.alias("target").merge(

        source_df_bk.alias("source"),

        _merge_condition
        ).whenMatchedUpdate(
            # close old record (new record with updated values gets inserted later)
            condition=_update_condition,
            set={
                "m_geldig_tot": source_validity_timestamp,  # Close old record
                "m_is_actief": F.lit(False),  # Deactivate old record
                "m_bijgewerkt_op": F.current_timestamp(), # Wijzigings datum
                "m_runid": F.lit(M_RUNID) # Runid bij voorkeur gevuld door variable
            }
        )
    
        # Conditionally close deleted records
        if close_deleted_records:
            merge_step_1 = merge_step_1.whenNotMatchedBySourceUpdate(
            condition=(
                (F.col("target.m_is_actief") == True) 
            ),
            set={
                "m_geldig_tot": source_validity_timestamp,
                "m_is_actief": F.lit(False),
                "m_bijgewerkt_op": F.current_timestamp(), # Wijzigings datum
                "m_runid": F.lit(M_RUNID)  # Runid bij voorkeur gevuld door variable
            }
        )

        ## Step 2: Insert new active records, both new and updated
        merge_step_2 = target_table.alias("target").merge(
            
        source_df_bk.alias("source"),
        _merge_condition
        ).whenNotMatchedInsert(
            # insert new record with updated values for the matched rows
            values={
                **insert_columns, 
                "m_geldig_van": source_validity_timestamp, # New record valid from now
                "m_geldig_tot": F.to_timestamp(F.lit("9000-12-31 00:00:00"), "yyyy-MM-dd HH:mm:ss"),
                "m_is_actief": F.lit(True), # Activate new record
                "m_bijgewerkt_op": F.current_timestamp(), # Wijzigings datum
                "m_aangemaakt_op": F.current_timestamp(), 
                "m_runid": F.lit(M_RUNID)  # Runid bij voorkeur gevuld door variable
                }
        )

        try:
            # Note current version
            version_before = spark.sql(f"DESCRIBE HISTORY {target_table_name}").collect()[0]["version"]

            # Execute the first merge (closes old records)
            merge_step_1.execute()

            # # Execute the second merge (inserts new records)
            merge_step_2.execute()
        except Exception as e:
            # Rollback
            restore_table_with_retry(target_table_name, version_before)
            raise Exception(f"Error executing merge operation on Delta table '{target_table_name}': ({e}). Restored to previous version ({version_before}).") from e

#
# Function to generate the CREATE TABLE script for a history table
# functie voor genereren historie tabel obv bestaande tabel
def generate_history_table_script(spark , catalog_schema: str, source_table_name: str) -> str:
    """
    Generates a CREATE TABLE script for a new history table based on an existing source table.
    It adds 'sid_tablename INT' and 'bk_account STRING' as leading columns, and meta colums for validity of every row

    Args:
        catalog_schema (str): The catalog and schema where the tables reside (e.g., "my_catalog.my_schema").
        source_table_name (str): The name of the existing source table (e.g., "account").
        target_table_name (str): The name of the new history table to create (e.g., "hist_account").

    Returns:
        str: The generated CREATE TABLE SQL statement.
    """

    # Stap 1 Volledige naam van de bron-tabel
    full_source_table_name = f"{catalog_schema}.{source_table_name}"
    target_table_name = f"hist_{source_table_name}"
    full_target_table_name = f"{catalog_schema}.{target_table_name}"

    # Stap 2: Definieer de nieuwe kolommen die aan het begin moeten worden toegevoegd
    # stel de sid kolom zo in, in het schema zodat er altijd zelf een unieke waarde wordt gemaakt door het systeem
    kollommen_voor  = [
        f"sid_{source_table_name} BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)",
        f"bk_{source_table_name} string"
    ]

    kollommen_achter = [  
        "  m_geldig_van  timestamp",
        "  m_geldig_tot timestamp",
        "  m_is_actief boolean",
        "  m_bron string",
        "  m_runid string",
        "  m_aangemaakt_op timestamp",
        "  m_bijgewerkt_op timestamp"
    ]

    source_table_schema = spark.table(full_source_table_name).schema

    # Stap 3: Verwerk alle kolommen van de bron-tabel en hernoem waar nodig
    processed_source_columns = []
    for field in source_table_schema:
        original_column_name = field.name
        data_type = field.dataType.simpleString().upper() # Haal het datatype op (bijv. 'STRING', 'TIMESTAMP')

        # Quote kolomnamen met speciale tekens
        if "@" in original_column_name:
            original_column_name = f"`{original_column_name}`"
        
        processed_source_columns.append(f"  {original_column_name} {data_type}")

    # Stap 4: Combineer alle kolommen
    all_columns = kollommen_voor + processed_source_columns + kollommen_achter

    # Stap 5: Construeer het CREATE TABLE statement 
    # Bouw de SQL-string lijn voor lijn op om f-string expressie fouten te voorkomen
    sql_lines = []
    sql_lines.append(f"CREATE TABLE IF NOT EXISTS {full_target_table_name} (")
    sql_lines.append(",\n".join(all_columns)) # Hier wordt de lijst van kolommen samengevoegd
    sql_lines.append(")")
    sql_lines.append("USING DELTA;")

    create_table_statement = "\n".join(sql_lines)
    
    # Voer het script direct uit in Databricks
    spark.sql(create_table_statement)
    print(f"Tabel '{full_target_table_name}' succesvol aangemaakt als hist tabel (indien deze nog niet bestond).")

def escape_column_name(col_name: str) -> str:
    """
    Escape kolomnamen met backticks als ze speciale tekens bevatten of niet al geëscaped zijn.
    """
    if not col_name.startswith("`") or not col_name.endswith("`"):
        if any(char in col_name for char in ['@', '-', ' ', '`']) or not col_name.isidentifier():
            return f"`{col_name}`"
    return col_name

def perform_scd2_delta_merge(
    spark,
    source_df: DataFrame,
    target_table: str,
    merge_condition: str,
    update_columns: list,
    insert_columns: list,
    mutation_datetime_column: str,
    m_runid_value: str,
    m_aangemaakt_value: datetime,
    m_bijgewerkt_value: datetime,
    delete_indicator_column: str
) -> None:
    """
    Voert een SCD Type 2 merge uit op een Delta tabel.
    Sluit oude records af en voegt nieuwe actieve records in,
    waarbij de datums worden bepaald door kolommen uit het source DataFrame.

    Args:
        source_df (DataFrame): Het DataFrame met de brongegevens.
        target_table (str): De naam van de Delta tabel die bijgewerkt moet worden.
        merge_condition (str): De conditie om records tussen source en target te matchen.
        update_columns (list): Een lijst van kolomnamen die gecontroleerd moeten worden op wijzigingen.
                                Als een van deze kolommen verschilt, wordt het record bijgewerkt.
        insert_columns (list): Een lijst van kolomnamen waarvan de waarden direct
                                uit het source_df moeten worden overgenomen voor nieuwe records.
        mutation_datetime_column (str): De naam van de kolom in source_df die de wijzigingsdatum/tijd bevat
                                        voor het openen van nieuwe records en sluiten van verwijderde records.
        m_runid_value (str): De waarde voor de m_runid kolom (een string, bijv. van de starttijd van de run).
        m_aangemaakt_value (datetime): De timestamp voor de m_aangemaakt_op kolom (vast voor de hele run/batch).
        m_bijgewerkt_value (datetime): De timestamp voor de m_bijgewerkt_op kolom (vast voor de hele run/batch).
        delete_indicator_column (str): De naam van de kolom in de target tabel die de verwijderingsindicator bevat.
                                        Deze kolom wordt gebruikt om te bepalen of een record verwijderd is en historisch dus afgesloten moet worden.
    """
    try:
        delta_table = DeltaTable.forName(spark, target_table)
    except Exception as e:
        raise ValueError(f"Fout bij het laden van Delta tabel '{target_table}': {e}")

    # --- Voorbereidingen ---
    
    # 1. Merge Conditie (Alleen actieve records in de target matchen op business key)
    _merge_condition = f"{merge_condition} AND target.m_is_actief = True"

    # 2. Escape de relevante kolommen
    escaped_mutation_datetime_column = escape_column_name(mutation_datetime_column)
    escaped_delete_col = escape_column_name(delete_indicator_column)
    
    # 3. Wijzigingsdetectie Conditie
    update_conditions_list = []
    for col_name in update_columns:
        escaped_col_name = escape_column_name(col_name)
        update_conditions_list.append(
            f"(target.{escaped_col_name} IS DISTINCT FROM source.{escaped_col_name})"
        )
    # De SCD2 update vindt plaats als er een wijziging is in een van de update_columns
    _update_condition_scd2 = f"({' OR '.join(update_conditions_list)})"

    # --- SCD2 MERGE Logica (Sluiting) ---
    
    # Start de merge builder
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"),
        _merge_condition
    )

    # 1. SLUITEN VAN VERWIJDERDE RECORDS (Eerste whenMatchedUpdate)
    # Sluit de rij af als de source aangeeft dat deze verwijderd is (verwijdering heeft prioriteit)
    merge_builder = merge_builder.whenMatchedUpdate(
        condition=f"source.{escaped_delete_col} = True AND target.m_is_actief = True",
        set={
            "m_geldig_tot": F.col(f"source.{escaped_mutation_datetime_column}"), # Datum van mutatie
            "m_is_actief": F.lit(False),
            "m_bijgewerkt_op": F.lit(m_bijgewerkt_value).cast(TimestampType())
        }
    )

    # 2. SLUITEN VAN GEWIJZIGDE RECORDS (Tweede whenMatchedUpdate)
    # Sluit de rij af als de attributen gewijzigd zijn EN de rij NIET is gemarkeerd als verwijderd
    scd2_update_condition = _update_condition_scd2
    scd2_update_condition += f" AND source.{escaped_delete_col} = False" # Voorkomt dubbele sluiting
    
    merge_builder = merge_builder.whenMatchedUpdate(
        condition=scd2_update_condition,
        set={
            "m_geldig_tot": F.col(f"source.{escaped_mutation_datetime_column}"),
            "m_is_actief": F.lit(False),
            "m_bijgewerkt_op": F.lit(m_bijgewerkt_value).cast(TimestampType())
        }
    )

    # Voer de complete merge uit om de sluitingen af te handelen
    merge_builder.execute() 

    # --- INVOEGEN Logica (nieuwe of gewijzigde actieve records) ---

    # Creëer een dictionary van kolommen en hun source-waarden voor de insert
    insert_values_from_source = {}
    for col_name in insert_columns:
        escaped_col_name = escape_column_name(col_name)
        insert_values_from_source[col_name] = F.col(f"source.{escaped_col_name}")

    # Insert Conditie: Alleen invoegen als het record NIET gemarkeerd is als verwijderd
    insert_condition = f"source.{escaped_delete_col} = False"

    delta_table.alias("target").merge(
        source_df.alias("source"),
        _merge_condition
    ).whenNotMatchedInsert(
        condition=insert_condition, 
        values={
            **insert_values_from_source,
            "m_geldig_van": F.col(f"source.{escaped_mutation_datetime_column}"),
            "m_geldig_tot": F.to_timestamp(F.lit("9000-12-31 00:00:00"), "yyyy-MM-dd HH:mm:ss"),
            "m_is_actief": F.lit(True),
            "m_bron": F.lit("MS CRM"),
            "m_runid": F.lit(m_runid_value),
            "m_aangemaakt_op": F.lit(m_aangemaakt_value).cast(TimestampType()),
            "m_bijgewerkt_op": F.lit(m_bijgewerkt_value).cast(TimestampType())
        }
    ).execute()

def genereer_scd_tijd_combinaties_per_key(tables: Dict[str, Dict[str, Any]], key_col: str) -> DataFrame:
    """
    Combineert meerdere SCD Type 2-tabellen en genereert opeenvolgende tijdsintervallen per business key.
    Gebruikt alleen Spark-native functies (geen UDFs).

    Parameters:
    tables (Dict[str, Dict[str, Any]]): Dictionary van tabellen, met per entry:
        - "df": DataFrame met kolommen m_geldig_van, m_geldig_tot en een key-kolom
        - "key": de naam van de business key kolom in de DataFrame
    key_col (str): Genormaliseerde key-kolomnaam in de output.

    Returns:
    DataFrame: [key_col, m_geldig_van, m_geldig_tot]
    """

    all_dates = []

    for _, table_info in tables.items():
        df = table_info["df"]
        key = table_info["key"]

        dates = df.select(
            F.col(key).alias(key_col),
            F.col("m_geldig_van").alias("datum")
        ).union(
            df.select(
                F.col(key).alias(key_col),
                F.col("m_geldig_tot").alias("datum")
            )
        )

        all_dates.append(dates)

    # Combineer alle datums
    combined_dates = reduce(lambda d1, d2: d1.unionByName(d2), all_dates)

    # Unieke datums per key
    unique_dates = combined_dates.distinct().orderBy(key_col, "datum")

    # Gebruik Window + lead om van datums intervallen te maken
    w = Window.partitionBy(key_col).orderBy("datum")

    result = unique_dates.withColumn("m_geldig_van", F.col("datum")) \
                         .withColumn("m_geldig_tot", F.lead("datum").over(w)) \
                         .drop("datum") \
                         .where(F.col("m_geldig_tot").isNotNull())

    return result