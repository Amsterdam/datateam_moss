# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

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

def haal_df_act_hist_op(
    spark,
    OBJECT_SID: str,
    OBJECT_BK: str,
    ACT_TABEL: str,
    HIST_TABEL: str,
    CATALOG: str,
    TARGET_SCHEMA: str
    ):

   
    # De kolommen die je wilt UITSLUITEN van de vergelijking
    EXCLUDE_COLUMNS = [OBJECT_SID,OBJECT_BK,"m_geldig_van","m_geldig_tot",
        "m_is_actief",
        "m_bron",
        "m_runid",
        "m_aangemaakt_op",
        "m_bijgewerkt_op",
        "@odata_etag",
        "mtd_timestamp_apicall",
        "modifiedon",
        "modifiedon@odata_community_display_v1_formattedvalue"
        ]
    # Laad de tabellen
    actueel_df = spark.table(f"{CATALOG}.{TARGET_SCHEMA}.{ACT_TABEL}")
    historisch_df = spark.table(f"{CATALOG}.{TARGET_SCHEMA}.{HIST_TABEL}").filter("m_is_actief = true")

    # Stap 1: Converteer de kolom naar een timestamp
    
    historisch_df = historisch_df.withColumn(
        "createdon@odata_community_display_v1_formattedvalue",
        F.to_timestamp("createdon@odata_community_display_v1_formattedvalue", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    # Stap 2: Formatteer de timestamp naar het gewenste formaat
    historisch_df = historisch_df.withColumn(
        "createdon@odata_community_display_v1_formattedvalue",
        F.date_format("createdon@odata_community_display_v1_formattedvalue", "d-M-yyyy HH:mm")
    )
    if OBJECT_BK != 'bk_accounts':
        historisch_df = historisch_df.withColumn(
            'createdon',
            F.date_format(
                F.to_timestamp('createdon', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                    "yyyy-MM-dd'T'HH:mm:00'Z'"
                )
        )
        actueel_df= actueel_df.withColumn(
            'createdon',
             F.date_format(
                F.to_timestamp('createdon', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                    "yyyy-MM-dd'T'HH:mm:00'Z'"
                )
        )   


    # Bepaal dynamisch de lijst met kolommen die vergeleken moeten worden
    # We gaan uit van de kolommen in de Actuele tabel
    all_actueel_columns = actueel_df.columns
    COMPARISON_COLUMNS = [c for c in all_actueel_columns if c not in EXCLUDE_COLUMNS]

    all_hist_columns = historisch_df.columns
    hist_COLUMNS = [c for c in all_hist_columns if c not in EXCLUDE_COLUMNS]

    print(f"Aantal te vergelijken kolommen: {len(COMPARISON_COLUMNS)}")
    print(f"Aantal te vergelijken kolommen: {len(hist_COLUMNS)}")
  
    # Selecteer alleen de te vergelijken kolommen in beide DataFrames
    actueel_df_selected = actueel_df.select(*COMPARISON_COLUMNS)
    historisch_df_selected = historisch_df.select(*COMPARISON_COLUMNS)

     # Sla de kolomlijsten op
    actueel_cols = actueel_df_selected.columns
    historisch_cols = historisch_df_selected.columns

    # 1. Controleer of het aantal kolommen gelijk is (snelle check)
    if len(actueel_cols) != len(historisch_cols):
        print("❌ FOUT: Het aantal kolommen is NIET gelijk.")
    else:
        # 2. Controleer of de inhoud van de lijsten IDENTIEK is (volgorde is niet belangrijk)
        # Door sets te gebruiken, controleer je snel op identieke inhoud.
        if set(actueel_cols) == set(historisch_cols):
            print("✅ De kolomnamen zijn IDENTIEK.")
        else:
            print("❌ FOUT: De kolomnamen zijn NIET gelijk (andere namen gevonden).")
            
            # Optioneel: Toon welke kolommen missen in de ene of de andere
            ontbrekend_in_historisch = set(actueel_cols) - set(historisch_cols)
            ontbrekend_in_actueel = set(historisch_cols) - set(actueel_cols)

            if ontbrekend_in_historisch:
                print(f"   Kolommen die in Actueel zitten, maar missen in Historisch: {ontbrekend_in_historisch}")
            if ontbrekend_in_actueel:
                print(f"   Kolommen die in Historisch zitten, maar missen in Actueel: {ontbrekend_in_actueel}")


    # Controleer of het aantal rijen gelijk is (Snelle check)
    actueel_count = actueel_df_selected.count()
    historisch_count = historisch_df_selected.count()

    print(f"\nAantal rijen Actueel: {actueel_count}")
    print(f"Aantal rijen Historisch (Gefilterd): {historisch_count}")

    if actueel_count != historisch_count:
        print("❌ FOUT: Aantal rijen is NIET gelijk. De data is niet identiek.")
        # Stop hier als de aantallen al verschillen
    else:
        print("✅ Aantal rijen is gelijk. Ga verder met de datavergelijking (Hash).")

    return actueel_df_selected, historisch_df_selected,

def generate_mismatches_report(
    actueel_df: DataFrame, 
    historisch_df: DataFrame, 
    primary_key: str,
    ) -> DataFrame:
    """
    Vergelijkt twee DataFrames rij-voor-rij en retourneert een rapport 
    met ALLEEN de rijen en kolommen die afwijken.

    Args:
        actueel_df: Het DataFrame van de actuele data (reeds geselecteerde kolommen).
        historisch_df: Het DataFrame van de historische data (reeds geselecteerde kolommen).
        primary_key: De naam van de kolom die als primaire sleutel dient.

    Returns:
        Een DataFrame met de primaire sleutel, de afwijkende kolomnaam, 
        de actuele waarde en de historische waarde (één rij per afwijking).
    """




    # --- STAP 1: Bepaal Kolommen en Voer FULL OUTER JOIN uit ---
    
    comparison_columns = [c for c in actueel_df.columns if c != primary_key]
    
    # FULL OUTER JOIN: nodig om zowel verschillen in waarden als ontbrekende/nieuwe records te zien.
    df_comparison = actueel_df.alias("A").join(
        historisch_df.alias("H"),
        F.col(f"A.{primary_key}") == F.col(f"H.{primary_key}"),
        "fullouter"
    )
    
    # --- STAP 2: Genereer de status- en waardekolommen (voor de filtering) ---

    select_expressions = [F.col(f"A.{primary_key}").alias(primary_key)]
    mismatch_checks = [] # Voor het uiteindelijke filter
    
    for c in comparison_columns:
        
        # Expressie om de status (MATCH/MISMATCH) te bepalen
        # IS DISTINCT FROM-logica: A.val != H.val, ook bij NULLs
        status_expr = F.when(
            (F.col(f"A.{c}").cast("string") != F.col(f"H.{c}").cast("string")) |
            (F.col(f"A.{c}").isNull() & F.col(f"H.{c}").isNotNull()) |
            (F.col(f"A.{c}").isNotNull() & F.col(f"H.{c}").isNull()),
            F.lit("MISMATCH")
        ).otherwise(F.lit("MATCH")).alias(f"{c}_status")
        
        # Voeg de drie kolommen toe aan de selectie
        select_expressions.append(F.col(f"A.{c}").alias(f"{c}_actueel"))
        select_expressions.append(F.col(f"H.{c}").alias(f"{c}_historisch"))
        select_expressions.append(status_expr)
        
        mismatch_checks.append(F.col(f"{c}_status") == F.lit("MISMATCH"))

    # DataFrame met alle status- en waardekolommen
    df_report_all = df_comparison.select(*select_expressions)
    
    # --- STAP 3: Filter op afwijkende rijen ---

    # Check voor ontbrekende records (sleutel bestaat niet in de andere tabel)
    missing_records_check = (F.col(f"A.{primary_key}").isNull()) | (F.col(f"H.{primary_key}").isNull())
    
    # Combineer alle checks in één filter: (ontbrekend) OF (een van de statussen is MISMATCH)
    final_filter = missing_records_check | mismatch_checks[0] 
    for check in mismatch_checks[1:]:
        final_filter = final_filter | check
        
    df_diff_rows = df_report_all.filter(final_filter)
    
    # --- STAP 4: Transformeer om ALLEEN de MISMATCH kolommen te tonen (Explode) ---

    mismatch_array_elements = []
    
    for c in comparison_columns:
        # Maak een Array met de afwijkende waardes (alleen als de status MISMATCH is)
        mismatch_struct = F.when(
            F.col(f"{c}_status") == F.lit("MISMATCH"),
            F.array(
                F.lit(c),                                     # De afwijkende kolomnaam
                F.col(f"{c}_actueel").cast("string"),          # De actuele waarde
                F.col(f"{c}_historisch").cast("string")        # De historische waarde
            )
        )
        mismatch_array_elements.append(mismatch_struct)

    # Verzamel alle mismatch structs in één array-kolom
    df_transformed = df_diff_rows.withColumn(
        "mismatches_array", 
        F.array(*mismatch_array_elements)
    )

    # Explodeer de array om één rij per afwijkende kolom te krijgen
    df_exploded = df_transformed.select(
        F.col(primary_key),
        F.explode(F.col("mismatches_array")).alias("mismatch_details")
    )
    
    # Finaliseer de selectie
    df_final = df_exploded.select(
        F.col(primary_key),
        F.col("mismatch_details").getItem(0).alias("afwijkende_kolom"),
        F.col("mismatch_details").getItem(1).alias("waarde_actueel"),
        F.col("mismatch_details").getItem(2).alias("waarde_historisch")
    )
    df_final = df_final.filter(F.col("afwijkende_kolom").isNotNull())
    
    return df_final.orderBy(primary_key, "afwijkende_kolom")