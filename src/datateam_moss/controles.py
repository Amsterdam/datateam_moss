# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List, Tuple, Optional
from pyspark.sql.window import Window # Importeer de Window specificatie

def check_nrow_tabel_vs_distinct_id(spark: SparkSession, tabelnaam: str, id: str):
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
        df.join(F.broadcast(df.dropDuplicates([kolom])), kolom, "inner")
        .select(kolom, F.count(kolom).over(window_spec).alias("count"))
        .filter(F.col("count") > 1)
        .distinct()
    )
    
    # If-statement om te controleren of er dubbele business_keys zijn
    if (df_with_counts.isEmpty()):
        print(f"Er zijn geen dubbele waarden gedetecteerd in de opgegeven kolom ({kolom}) van de tabel.")  
    else:
        raise ValueError(f"Niet alle waarden in de kolom '{kolom}' zijn uniek.")
    return



def vergelijk_origineel_nieuw_tbl(
    spark: SparkSession,
    OBJECT_KEY_COLS: List[str], # Lijst van sleutelkolommen (SID/BK)
    ORG_TABEL: str,
    NEW_TABEL: str,
    CATALOG: str,
    TARGET_SCHEMA: str,
    # Nieuwe Optionele Parameters voor Filters
    ORG_FILTER: Optional[str] = None, 
    NEW_FILTER: Optional[str] = None,
    EXCLUDE_COLUMNS_LIST: Optional[List[str]] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Vergelijkt de structuur en het rij-aantal van twee PySpark DataFrames (Origineel en Nieuw) 
    na het uitsluiten van specifieke metadatavelden en het toepassen van optionele filters.

    Args:
        spark: De actieve SparkSession.
        OBJECT_KEY_COLS: Een lijst met kolomnamen die de unieke sleutel(s) vormen 
                         (worden ALTIJD uitgesloten van de data-vergelijking).
        ORG_TABEL: De naam van de Originele (oude) tabel.
        NEW_TABEL: De naam van de Nieuwe (huidige) tabel.
        CATALOG: De naam van de Spark Catalogus.
        TARGET_SCHEMA: De naam van het Schema in de Catalogus.
        ORG_FILTER: Een OPTIONELE SQL WHERE-clausule om de Originele tabel te filteren 
                    (bv. "m_is_actief = true"). Standaard is geen filter.
        NEW_FILTER: Een OPTIONELE SQL WHERE-clausule om de Nieuwe tabel te filteren. 
                    Standaard is geen filter.
        EXCLUDE_COLUMNS_LIST: Een OPTIONELE lijst met extra kolomnamen die moeten worden
                              uitgesloten van de vergelijking (meestal technische kolommen).

    Returns:
        Een tuple van (origineel_df_geselecteerd, nieuw_df_geselecteerd).
        Dit zijn de DataFrames met enkel de kolommen die vergeleken moeten worden.
    """
    
    # 1. Bepaal de definitieve lijst met uit te sluiten kolommen
    # Gebruik de meegegeven sleutelkolommen als verplichte uitsluiting
    STANDAARD_EXCLUDE = OBJECT_KEY_COLS
    
    user_excludes = EXCLUDE_COLUMNS_LIST if EXCLUDE_COLUMNS_LIST is not None else []
        
    # Combineer alle uit te sluiten kolommen
    EXCLUDE_COLUMNS = list(set(STANDAARD_EXCLUDE + user_excludes))
    
    print(f"Kolommen die worden uitgesloten van vergelijking (Keys + Extra): {EXCLUDE_COLUMNS}")

    # 2. Laad de tabellen en pas de optionele filters toe
    
    # Originele (ORG) Tabel
    org_df = spark.table(f"{CATALOG}.{TARGET_SCHEMA}.{ORG_TABEL}")
    if ORG_FILTER:
        print(f"Filter toegepast op {ORG_TABEL} (Origineel): {ORG_FILTER}")
        org_df = org_df.filter(ORG_FILTER)
    else:
        print(f"Geen filter toegepast op {ORG_TABEL} (Origineel).")

    # Nieuwe (NEW) Tabel
    new_df = spark.table(f"{CATALOG}.{TARGET_SCHEMA}.{NEW_TABEL}")
    if NEW_FILTER:
        print(f"Filter toegepast op {NEW_TABEL} (Nieuw): {NEW_FILTER}")
        new_df = new_df.filter(NEW_FILTER)
    else:
        print(f"Geen filter toegepast op {NEW_TABEL} (Nieuw).")
    
    # 3. Selecteer de te vergelijken kolommen
    
    # We gaan uit van de kolommen in de NIEUWE tabel
    all_new_columns = new_df.columns
    COMPARISON_COLUMNS = [c for c in all_new_columns if c not in EXCLUDE_COLUMNS]

    new_df_selected = new_df.select(*COMPARISON_COLUMNS)
    
    # Zorg ervoor dat de Originele DF dezelfde, bestaande kolommen selecteert
    all_org_columns = org_df.columns
    ORG_COMPARISON_COLUMNS = [c for c in all_org_columns if c in COMPARISON_COLUMNS]
    
    # Zorg ervoor dat de selectie in de Originele tabel dezelfde volgorde heeft als Nieuw
    org_df_selected = org_df.select(*ORG_COMPARISON_COLUMNS)

    print(f"\nAantal kolommen in Nieuw voor vergelijking: {len(new_df_selected.columns)}")
    print(f"Aantal kolommen in Origineel voor vergelijking: {len(org_df_selected.columns)}")

    # Sla de kolomlijsten op
    new_cols = new_df_selected.columns
    org_cols = org_df_selected.columns

    # 4. Controleer de KOLOMNAMEN
    if set(new_cols) == set(org_cols):
        print("✅ De kolomnamen zijn IDENTIEK.")
        if new_cols != org_cols:
             print("⚠️ Opmerking: Kolomvolgorde is verschillend.")
    else:
        print("❌ FOUT: De kolomnamen zijn NIET gelijk.")
        
        # Toon welke kolommen missen in de ene of de andere
        ontbrekend_in_org = set(new_cols) - set(org_cols)
        ontbrekend_in_new = set(org_cols) - set(new_cols)

        if ontbrekend_in_org:
            print(f"   Kolommen die in Nieuw zitten, maar missen in Origineel: {ontbrekend_in_org}")
        if ontbrekend_in_new:
            print(f"   Kolommen die in Origineel zitten, maar missen in Nieuw: {ontbrekend_in_new}")


    # 5. Controleer het AANTAL RIJEN
    new_count = new_df_selected.count()
    org_count = org_df_selected.count()

    print(f"\nAantal rijen Nieuw (Gefilterd): {new_count}")
    print(f"Aantal rijen Origineel (Gefilterd): {org_count}")

    if new_count != org_count:
        print("❌ FOUT: Aantal rijen is NIET gelijk.")
    else:
        print("✅ Aantal rijen is gelijk. Ga verder met de datavergelijking (bv. Hash).")

    return org_df_selected, new_df_selected



def genereer_afwijkingen_rapport(
    org_df: DataFrame, 
    new_df: DataFrame, 
    OBJECT_KEY_COLS: List[str],
) -> DataFrame:
    """
    Vergelijkt twee DataFrames (Origineel/Org en Nieuw/New) rij-voor-rij en retourneert een 
    rapport met ALLEEN de rijen en kolommen die afwijken.

    Deze functie behandelt verschillen in waarden, types, en records die ontbreken 
    in één van de DataFrames na een Full Outer Join.

    Args:
        org_df: Het DataFrame van de Originele (oude) data (reeds geselecteerde kolommen).
        new_df: Het DataFrame van de Nieuwe (huidige) data (reeds geselecteerde kolommen).
        OBJECT_KEY_COLS: Een lijst met kolomnamen die de unieke sleutel(s) vormen 
                         (worden gebruikt voor de JOIN en uitsluitend in het rapport).

    Returns:
        Een DataFrame met de sleutelkolommen, de afwijkende kolomnaam, 
        de Nieuwe waarde en de Originele waarde (één rij per afwijking).
    """

    # We gaan ervan uit dat de kolommen in new_df en org_df identiek zijn (resultaat van vergelijk_origineel_nieuw_dfs)
    # De sleutelkolommen worden uitgesloten van de WAARDE-vergelijking
    comparison_columns = [c for c in new_df.columns if c not in OBJECT_KEY_COLS]
    
    # --- STAP 1: Bepaal JOIN conditie en voer FULL OUTER JOIN uit ---
    
    # Maak de JOIN-conditie aan voor de lijst met sleutelkolommen
    join_condition = (F.col(f"N.{OBJECT_KEY_COLS[0]}") == F.col(f"O.{OBJECT_KEY_COLS[0]}"))
    for key_col in OBJECT_KEY_COLS[1:]:
        join_condition = join_condition & (F.col(f"N.{key_col}") == F.col(f"O.{key_col}"))
    
    # FULL OUTER JOIN: nodig om verschillen, ontbrekende en nieuwe records te zien.
    df_comparison = new_df.alias("N").join(
        org_df.alias("O"),
        join_condition,
        "fullouter"
    )
    
    # --- STAP 2: Genereer de status- en waardekolommen (voor de filtering) ---

    # Selecteer alle sleutelkolommen. Gebruik de sleutel van 'N' (Nieuw) en coalesce met 'O' (Origineel)
    # om de sleutel te krijgen bij ontbrekende records (bij full outer join)
    select_expressions = [
        F.coalesce(F.col(f"N.{c}"), F.col(f"O.{c}")).alias(c) 
        for c in OBJECT_KEY_COLS
    ]
    mismatch_checks = [] # Voor het uiteindelijke filter
    
    for c in comparison_columns:
        
        # Expressie om de status (MATCH/MISMATCH) te bepalen
        # Gebruik F.lit(None).cast(df_comparison.select(f"N.{c}").dtypes[0][1]) om NULLs te vergelijken
        # IS DISTINCT FROM-logica: A.val != H.val, inclusief NULLs
        
        # We casten naar string om data type verschillen in de vergelijking te negeren, 
        # tenzij ze de stringwaarde veranderen (wat meestal ongewenst is in een pure data vergelijking).
        status_expr = F.when(
            (F.col(f"N.{c}").cast("string").isNotNull() & F.col(f"O.{c}").cast("string").isNull()) | # N is niet null, O is null
            (F.col(f"N.{c}").cast("string").isNull() & F.col(f"O.{c}").cast("string").isNotNull()) | # N is null, O is niet null
            (F.col(f"N.{c}").cast("string") != F.col(f"O.{c}").cast("string")),                      # Waarden verschillen
            F.lit("MISMATCH")
        ).otherwise(F.lit("MATCH")).alias(f"{c}_status")
        
        # Voeg de drie kolommen toe aan de selectie
        select_expressions.append(F.col(f"N.{c}").alias(f"{c}_nieuw"))
        select_expressions.append(F.col(f"O.{c}").alias(f"{c}_origineel"))
        select_expressions.append(status_expr)
        
        mismatch_checks.append(F.col(f"{c}_status") == F.lit("MISMATCH"))

    # DataFrame met alle status- en waardekolommen
    df_report_all = df_comparison.select(*select_expressions)
    
    # --- STAP 3: Filter op afwijkende rijen ---

    # Check voor ontbrekende records (sleutel in N of O is NULL)
    # Dit werkt omdat de sleutelkolommen in 'select_expressions' zijn samengevoegd (coalesce)
    missing_records_check = (
        F.col(f"{OBJECT_KEY_COLS[0]}").isNull() # Zou niet mogen gebeuren na coalesce als er data is
    )

    # Combineer alle checks in één filter: (een van de statussen is MISMATCH)
    # Ontbrekende rijen worden opgevangen door de mismatch-checks: alle waarden zijn NULL/MISMATCH
    
    if not mismatch_checks:
        # Geen vergelijkingskolommen gevonden, retourneer een leeg resultaat met de juiste structuur
        print("WAARSCHUWING: Geen kolommen gevonden om te vergelijken na het uitsluiten van sleutels.")
        empty_schema = [F.col(c) for c in OBJECT_KEY_COLS] + \
                       [F.lit(None).alias("afwijkende_kolom"), F.lit(None).alias("waarde_nieuw"), F.lit(None).alias("waarde_origineel")]
        return spark.createDataFrame([], schema=df_comparison.select(*empty_schema).schema).limit(0)

    final_filter = mismatch_checks[0] 
    for check in mismatch_checks[1:]:
        final_filter = final_filter | check
        
    df_diff_rows = df_report_all.filter(final_filter)
    
    # --- STAP 4: Transformeer om ALLEEN de MISMATCH kolommen te tonen (Explode) ---

    mismatch_array_elements = []
    
    for c in comparison_columns:
        # Maak een Struct met de afwijkende waardes (alleen als de status MISMATCH is)
        mismatch_struct = F.when(
            F.col(f"{c}_status") == F.lit("MISMATCH"),
            F.struct(
                F.lit(c).alias("kolom"),
                F.col(f"{c}_nieuw").cast("string").alias("nieuw"),
                F.col(f"{c}_origineel").cast("string").alias("origineel")
            )
        )
        mismatch_array_elements.append(mismatch_struct)

    # Verzamel alle mismatch structs in één array-kolom
    df_transformed = df_diff_rows.withColumn(
        "mismatches_array", 
        F.array_except(F.array(*mismatch_array_elements), F.array(F.lit(None))) # Verwijder de NULL structs
    )

    # Explodeer de array om één rij per afwijkende kolom te krijgen
    df_exploded = df_transformed.select(
        *OBJECT_KEY_COLS,
        F.explode(F.col("mismatches_array")).alias("mismatch_details")
    )
    
    # Finaliseer de selectie
    df_final = df_exploded.select(
        *OBJECT_KEY_COLS,
        F.col("mismatch_details").getItem("kolom").alias("afwijkende_kolom"),
        F.col("mismatch_details").getItem("nieuw").alias("waarde_nieuw"),
        F.col("mismatch_details").getItem("origineel").alias("waarde_origineel")
    )
    
    # Sorteer en retourneer
    return df_final.orderBy(*OBJECT_KEY_COLS, "afwijkende_kolom")