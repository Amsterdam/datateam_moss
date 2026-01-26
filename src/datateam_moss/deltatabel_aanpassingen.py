from pyspark.sql.functions import col, lit
from pyspark.sql.utils import AnalysisException # Importeer de specifieke uitzondering voor SQL-fouten
import sys

# 
# Hier staan generieke functies die gebruikt worden om aanpasingen te doen aan tabellen en kollommen via eenmalige
# scripts. Deze functies zijn ontworpen zodat dit door de hele OTAP straat op uitgeteste en gecontrolleerde wijze kan plaatsvinden
#
#>> FUNCTIES IN DEZE FILE ZIJN:
#
# 1 hernoem_kolommen_met_delta_mapping
# 2 voeg_kolommen_toe
# 3 _get_default_literal
# 4 vervang_tabel_met_tijdelijke_tabel
# 5 voeg_kolommen_toe_op_positie
# 6 pas_datatypes_aan
# 7 verwijder_kolommen_uit_delta_tabel
# 8 pas_kolomvolgorde_aan
# 9 update_table_from_other_table
#

#
#> 1 HERNOEM KOLLOMMEN MET DELTA MAPPING
#

def hernoem_kolommen_met_delta_mapping(spark, catalog, target_schema, tabel_kolom_rename_mapping):
    """
    Hernoemt kolommen in meerdere tabellen en schakelt Delta column mapping in.
    Geeft een foutmelding als een kolom niet bestaat, maar blijft de rest van de kolommen verwerken.

    Parameters:
    - spark: De SparkSession (vereist in Databricks).
    - catalog: De naam van de catalogus.
    - target_schema: De naam van het doel-schema.
    - tabel_kolom_rename_mapping: Een dictionary waarin de sleutels de tabelnamen zijn
      en de waarden dictionaries zijn met oude kolomnamen als sleutels en nieuwe kolomnamen als waarden.
    """
    for originele_tabel, kolom_mapping in tabel_kolom_rename_mapping.items():
        # Definieer de volledige naam van de tabel
        volledige_tabel_naam = f"{catalog}.{target_schema}.{originele_tabel}"
        
        print(f"\nBezig met tabel: {volledige_tabel_naam}")

        # 1. Schakel Delta column mapping in voor de tabel
        try:
            spark.sql(f"""
                ALTER TABLE {volledige_tabel_naam} SET TBLPROPERTIES (
                    'delta.columnMapping.mode' = 'name',
                    'delta.minReaderVersion' = '2',
                    'delta.minWriterVersion' = '5'
                )
            """)
            print(f"✅ Delta column mapping ingeschakeld voor: {volledige_tabel_naam}")
        except AnalysisException as e:
            # Vang de AnalysisException op als de tabel niet bestaat of een andere fout optreedt bij TBLPROPERTIES
            print(f"❌ Fout bij het inschakelen van Delta column mapping voor {volledige_tabel_naam}: {e}")
            continue # Ga naar de volgende tabel als er een fout is bij het inschakelen van de mapping.

        # 2. Hernoem de kolommen in de tabel
        for oude_kolom, nieuwe_kolom in kolom_mapping.items():
            try:
                spark.sql(f"""
                    ALTER TABLE {volledige_tabel_naam} RENAME COLUMN `{oude_kolom}` TO `{nieuwe_kolom}`
                """)
                print(f"   ✅ Kolom hernoemd: **`{oude_kolom}`** -> **`{nieuwe_kolom}`**")
            except AnalysisException as e:
                # Vang de AnalysisException op die optreedt als de kolom niet bestaat
                # of een andere SQL-gerelateerde fout
                foutboodschap = getattr(e, 'errorMessage', str(e)) 
            
                print(f"   ⚠️ FOUT: Kolom **`{oude_kolom}`** kon NIET worden hernoemd in tabel **`{volledige_tabel_naam}`** naar **`{nieuwe_kolom}`**.")
                print(f"   Reden: {foutboodschap.split(':')[0]}") # Print een deel van de foutboodschap voor beknoptheid
                # De lus gaat hier verder naar de volgende kolom in de tabel
    
    print("\nAlle hernoemingen zijn verwerkt. Controleer de ⚠️ FOUT berichten voor mislukte renamen.")
    

#
#> 2 VOEG KOLLOMMEN TOE AAN DELTA TABEL
#

def voeg_kolommen_toe(spark, catalog, target_schema, kolom_toevoeg_mapping):
    """
    Voeg nieuwe kolommen toe aan Delta-tabellen.

    Parameters:
    - spark: De SparkSession (vereist in Databricks).
    - catalog: De naam van de catalogus.
    - target_schema: De naam van het doel-schema.
    - kolom_toevoeg_mapping: Een dictionary waarin de sleutels de tabelnamen zijn
      en de waarden dictionaries zijn met kolomnamen als sleutels en een tuple (datatype, nullable) als waarden.
    """
    for tabel, kolom_mapping in kolom_toevoeg_mapping.items():
        volledige_tabel_naam = f"{catalog}.{target_schema}.{tabel}"
        
        for kolom, (datatype, nullable) in kolom_mapping.items():
            # Bepaal of de kolom nullable is
            nullable_str = "NULL" if nullable else "NOT NULL"
            
            # SQL-query om de kolom toe te voegen
            spark.sql(f"""
                ALTER TABLE {volledige_tabel_naam}
                ADD COLUMNS ({kolom} {datatype} {nullable_str})
            """)
            print(f"Kolom toegevoegd: {kolom} ({datatype}, {nullable_str}) in tabel {volledige_tabel_naam}")

    print("Alle nieuwe kolommen zijn succesvol toegevoegd.")

#
#> 3 GET DEFAULT LITERAL
#

def _get_default_literal(datatype: str):
    """
    Bepaalt de standaardwaarde (literal) op basis van het datatype voor 
    nieuwe NOT NULL kolommen. Dit is cruciaal om te voorkomen dat 
    ALTER TABLE ... SET NOT NULL faalt.
    """
    datatype = datatype.upper()
    if datatype in ("STRING", "VARCHAR"):
        return lit("")
    elif datatype in ("INT", "INTEGER", "BIGINT", "SMALLINT"):
        return lit(0)
    elif datatype in ("DOUBLE", "FLOAT", "DECIMAL"):
        return lit(0.0)
    elif datatype == "BOOLEAN":
        return lit(False)
    elif datatype == "DATE":
        return lit("1900-01-01").cast("date") 
    elif datatype == "TIMESTAMP":
        # Gebruik de epoch datum/tijd als veilige standaard
        return lit("1970-01-01 00:00:00").cast("timestamp")
    else:
        # Voor onbekende of complexe types, val terug op NULL (waarschuwing: DDL zal falen!)
        print(f"WAARSCHUWING: Onbekend datatype '{datatype}' voor NOT NULL kolom. Gebruikt lit(None).")
        return lit(None)

#
#> 4 VERVANG TABEL MET TIJDELIJKE TABEL
#

def vervang_tabel_met_tijdelijke_tabel(
    spark, 
    oude_tabel_pad: str, 
    nieuwe_tabel_pad: str, 
    nieuwe_df, 
    nieuwe_kolom_definitie: dict
):
    """
    Vervangt een bestaande tabel door een nieuwe tijdelijke tabel en past NOT NULL constraints toe.
    Deze functie is aangepast om de DDL (ALTER TABLE) uit te voeren.
    """
    # Stap 1: Schrijf de nieuwe data naar de tijdelijke tabel
    print(f"Schrijft data naar tijdelijke tabel: {nieuwe_tabel_pad}")
    nieuwe_df.write.format("delta").mode("overwrite").saveAsTable(nieuwe_tabel_pad)
    
    # Optionele Stap: Controleer het aantal rijen
    try:
        # Probeer het aantal rijen te vergelijken (kan falen als oude_tabel_pad niet bestaat)
        originele_rijen_aantal = spark.table(oude_tabel_pad).count()
        nieuwe_rijen_aantal = spark.table(nieuwe_tabel_pad).count()
        if nieuwe_rijen_aantal != originele_rijen_aantal:
             raise ValueError(f"Aantal rijen in de nieuwe tabel ({nieuwe_rijen_aantal}) komt niet overeen met het aantal rijen in de originele tabel ({originele_rijen_aantal}).")
        print(f"Data succesvol overgezet naar tijdelijke tabel: {nieuwe_tabel_pad}. Rijentelling is correct ({nieuwe_rijen_aantal}).")
    except Exception as e:
        print(f"INFO: Kon rijen niet controleren voor {oude_tabel_pad} (fout: {e}). Gaat door.")

    # Stap 2: Verwijder de originele tabel
    print(f"Verwijdert originele tabel {oude_tabel_pad}...")
    spark.sql(f"DROP TABLE IF EXISTS {oude_tabel_pad}")

    # Stap 3: Hernoem de tijdelijke tabel naar de originele tabel
    print(f"Hernoemt tijdelijke tabel {nieuwe_tabel_pad} naar {oude_tabel_pad}")
    spark.sql(f"ALTER TABLE {nieuwe_tabel_pad} RENAME TO {oude_tabel_pad}")
    
    # Stap 4: Pas de NOT NULL constraints toe op de nieuwe kolommen
    print("Controleert op nieuwe kolommen met NOT NULL constraint...")
    for naam, (datatype, positie, nullable) in nieuwe_kolom_definitie.items():
        if not nullable:
            # Dit DDL-commando werkt op Delta Lake en de meeste Spark SQL-implementaties.
            alter_sql = f"ALTER TABLE {oude_tabel_pad} ALTER COLUMN {naam} SET NOT NULL"
            
            print(f"-> Voert DDL uit voor NOT NULL op kolom {naam}: {alter_sql}")
            try:
                spark.sql(alter_sql)
            except Exception as e:
                # Als de DDL faalt (bijv. omdat de standaardwaarde niet correct was, of de syntax niet wordt ondersteund)
                print(f"FOUT: Kon NOT NULL niet toepassen op kolom {naam} in {oude_tabel_pad}. Fout: {e}")

    # Stap 5: Vernieuw de metadata
    print(f"Vernieuwt de catalogus metadata voor tabel {oude_tabel_pad}...")
    spark.catalog.refreshTable(oude_tabel_pad)

    print(f"Tabel {oude_tabel_pad} succesvol bijgewerkt.")

#
#> 5 VOEG KOLLOMMEN TOE AAN DELTA TABEL OP POSITIE
#

def voeg_kolommen_toe_op_positie(spark, catalog: str, target_schema: str, kolom_toevoeg_mapping: dict):
    """
    Voegt nieuwe kolommen toe aan Delta-tabellen op een specifieke positie en dwingt 
    NOT NULL constraints af via DDL.
    """
    for tabel, kolom_mapping in kolom_toevoeg_mapping.items():
        # Definieer de volledige naam van de tabellen
        oude_tabel_pad = f"{catalog}.{target_schema}.{tabel}"
        nieuwe_tabel_pad = f"{oude_tabel_pad}_temp_met_nieuwe_kolommen"

        print(f"\n--- Start verwerking voor tabel: {oude_tabel_pad} ---")

        try:
            # Lees de data van de originele tabel in een DataFrame
            df = spark.table(oude_tabel_pad)
            originele_kolommen = [c.name for c in df.schema]
            
            # Bepaal de definitieve kolomvolgorde en de nieuwe kolommen
            nieuwe_kolom_definitie = {}
            for naam, datatype, positie, nullable in kolom_mapping:
                # Sla de definitie op voor later gebruik in de DDL stap
                nieuwe_kolom_definitie[naam] = (datatype, positie, nullable)

            definitieve_kolom_volgorde = []
            
            # Voeg de nieuwe kolommen in op de juiste positie
            for kolom_naam in originele_kolommen:
                # Kolommen VÓÓR de huidige kolom
                for nieuwe_kolom, (datatype, positie, nullable) in nieuwe_kolom_definitie.items():
                    if positie == kolom_naam:
                        definitieve_kolom_volgorde.append((nieuwe_kolom, datatype, nullable))
                
                # De Oude kolom zelf
                definitieve_kolom_volgorde.append((kolom_naam, None, None))

            # Voeg kolommen toe die aan het einde moeten komen (positie is None)
            for nieuwe_kolom, (datatype, positie, nullable) in nieuwe_kolom_definitie.items():
                if positie is None:
                    definitieve_kolom_volgorde.append((nieuwe_kolom, datatype, nullable))

            # Bouw de selectiekolommen met de juiste volgorde, types en (standaard)waarden
            kolommen_om_te_selecteren = []
            for naam, datatype, nullable in definitieve_kolom_volgorde:
                if datatype is None:  # Oude kolom
                    kolommen_om_te_selecteren.append(col(naam))
                else:  # Nieuwe kolom
                    if nullable:
                        # Nieuwe kolom is NULLABLE: voeg een NULL toe
                        kolommen_om_te_selecteren.append(lit(None).cast(datatype).alias(naam))
                    else:
                        # Nieuwe kolom is NOT NULL: voeg een STANDAARDWAARDE toe
                        standaard_waarde_lit = _get_default_literal(datatype)
                        kolommen_om_te_selecteren.append(standaard_waarde_lit.cast(datatype).alias(naam))

            # Selecteer de kolommen met de bijgewerkte datatypes in een nieuwe DataFrame
            nieuwe_df = df.select(kolommen_om_te_selecteren)
            
            print(f"Nieuw schema voor {tabel}:")
            nieuwe_df.printSchema()
            
            # Gebruik de helperfunctie om de tabel te vervangen en DDL toe te passen
            vervang_tabel_met_tijdelijke_tabel(
                spark, 
                oude_tabel_pad, 
                nieuwe_tabel_pad, 
                nieuwe_df, # Geef de nieuwe DF mee
                nieuwe_kolom_definitie # Geef de mapping mee voor DDL
            )

        except Exception as e:
            print(f"Fout opgetreden bij verwerking van tabel {oude_tabel_pad}: {e}", file=sys.stderr)
            raise

    print("\nAlle nieuwe kolommen zijn succesvol toegevoegd op de gewenste posities.")

#
# 6 PAS DATATYPES AAN VAN DELTA TABEL
#

def pas_datatypes_aan(spark, catalog, target_schema, kolom_data_type_mapping):
    """
    Wijzigt alleen de datatypes van opgegeven kolommen in Delta-tabellen.
    Kolommen die beginnen met 'sid_' worden uitgesloten van de gegevensoverdracht.
    Andere kolomeigenschappen, zoals AUTO INCREMENT, blijven behouden.

    Parameters:
    - spark: De SparkSession (vereist in Databricks).
    - catalog: De naam van de catalogus.
    - target_schema: De naam van het doel-schema.
    - kolom_data_type_mapping: Een dictionary waarin de sleutels de tabelnamen zijn
      en de waarden dictionaries zijn met kolomnamen als sleutels en hun nieuwe datatypes als waarden.
    """
    for originele_tabel, kolom_mapping in kolom_data_type_mapping.items():
        # Definieer de volledige namen van de tabellen
        oude_tabel_pad = f"{catalog}.{target_schema}.{originele_tabel}"
        nieuwe_tabel_pad = f"{oude_tabel_pad}_temp_nieuwe_datatypes"

        print(f"Start verwerking voor tabel: {oude_tabel_pad}")

        try:
            # Lees de data van de originele tabel in een DataFrame
            df = spark.table(oude_tabel_pad)

            # Bepaal welke kolommen behouden moeten worden en welke van datatype moeten veranderen
            kolommen_om_te_selecteren = []
            for kolom_naam in df.columns:
                # Kolommen die beginnen met 'sid_' worden overgeslagen
                if kolom_naam.lower().startswith("sid_"):
                    print(f"Kolom '{kolom_naam}' wordt overgeslagen (SID-kolom).")
                    kolommen_om_te_selecteren.append(col(kolom_naam))
                elif kolom_naam in kolom_mapping:
                    # Cast de kolom naar het nieuwe datatype
                    nieuw_type = kolom_mapping[kolom_naam]
                    print(f"Kolom '{kolom_naam}' wordt gecast naar '{nieuw_type}'.")
                    kolommen_om_te_selecteren.append(col(kolom_naam).cast(nieuw_type))
                else:
                    # Behoud de kolom zoals deze is
                    kolommen_om_te_selecteren.append(col(kolom_naam))

            # Selecteer de kolommen met de bijgewerkte datatypes in een nieuwe DataFrame
            nieuwe_df = df.select(*kolommen_om_te_selecteren)
            
           # Gebruik de nieuwe functie om de tabel te vervangen
            vervang_tabel_met_tijdelijke_tabel(spark, oude_tabel_pad, nieuwe_tabel_pad)

        except Exception as e:
            print(f"Fout opgetreden bij verwerking van tabel {oude_tabel_pad}: {e}")
            raise

    print("Alle opgegeven datatypes zijn succesvol aangepast.")

#
# 7 VERWIJDER KOLLOMMEN UIT DELTA TABEL
#

def verwijder_kolommen_uit_delta_tabel(spark, catalog: str, target_schema: str, kolom_verwijder_mapping: dict):
    """
    Verwijdert kolommen uit Delta-tabellen door een back-up te maken en de tabel opnieuw te creëren,
    waarna de nieuwe opgeschoonde tabel de oorspronkelijke tabel vervangt via hernoeming.
    
    Belangrijkste wijziging: Als de originele tabel een kolom bevatte die begint met 'sid_',
    dan wordt deze in de nieuwe tabel opnieuw aangemaakt als een automatisch gegenereerde 
    BIGINT IDENTITY kolom, via de correcte DDL-methode (CREATE TABLE + INSERT INTO).
    
    Belangrijk: Alle kolomnamen die speciale tekens bevatten (zoals '@') worden nu 
    automatisch geciteerd met backticks (`) in de gegenereerde SQL-statements om 
    Syntax Errors te voorkomen.

    Parameters:
    - spark: De SparkSession.
    - catalog: De naam van de catalogus.
    - target_schema: De naam van het doel-schema.
    - kolom_verwijder_mapping: Een dictionary waarin de sleutels de tabelnamen zijn
      en de waarden lijsten zijn van kolommen die moeten worden verwijderd.
    """
    
    # Gebruik een set voor snellere lookup van te verwijderen kolommen (niet direct gebruikt in de loop, maar goed voor opschoning)
    # te_verwijderen_kolommen_totaal = set()
    # for kolommen in kolom_verwijder_mapping.values():
    #     te_verwijderen_kolommen_totaal.update([k.lower() for k in kolommen]) # De totale set is niet langer nodig, we verwerken per tabel

    for originele_tabel_naam, kolommen_verwijderen in kolom_verwijder_mapping.items():
        # Definieer de volledige namen van de tabellen
        originele_tabel = f"{catalog}.{target_schema}.{originele_tabel_naam}"
        backup_tabel = f"{originele_tabel}_backup"
        nieuwe_tabel = f"{originele_tabel}_nw" 

        try:
            print(f"--- Start verwerking tabel: {originele_tabel} ---")

            # 1. Bepaal de kolommen van de originele tabel en check op sid_
            try:
                # Laad de tabel met een beperking (limit 0) om het schema snel te verkrijgen
                df_origineel = spark.table(originele_tabel) 
            except AnalysisException:
                print(f"Tabel {originele_tabel} bestaat niet. Overslaan.")
                continue

            originele_kolommen = df_origineel.columns
            # Maak een set van te verwijderen kolommen (in lowercase) voor snelle lookup
            kolommen_verwijderen_set = {k.lower() for k in kolommen_verwijderen}
            
            # Zoek de eerste 'sid_' kolom (er mag slechts één identity kolom zijn)
            sid_kolom_naam = next((
                kolom for kolom in originele_kolommen 
                if kolom.lower().startswith("sid_")
            ), None)

            if sid_kolom_naam:
                print(f"Opmerking: Identity kolom '{sid_kolom_naam}' gevonden. Deze wordt opnieuw aangemaakt in de nieuwe tabel met IDENTITY eigenschappen.")
            
            # 2. Bepaal de kolommen die BEHOUDEN moeten blijven in de data-overdracht
            # We sluiten alle 'sid_' kolommen en de kolommen uit de lijst uit.
            kolommen_behouden = [
                kolom for kolom in originele_kolommen
                if not kolom.lower().startswith("sid_") and kolom.lower() not in kolommen_verwijderen_set
            ]

            # 3. Selecteer de data (zonder de verwijderde kolommen en zonder sid_)
            df_gefilterd = df_origineel.select(*kolommen_behouden)
            print(f"Kolommen behouden voor data-overdracht: {kolommen_behouden}")

            # 4. Schrijf de gefilterde data naar de back-up tabel (VEILIGHEIDSSTAP)
            spark.sql(f"DROP TABLE IF EXISTS {backup_tabel}") # Zorg dat back-up schoon is
            df_gefilterd.write.format("delta").mode("overwrite").saveAsTable(backup_tabel)
            
            # Count checks
            # Gebruik de originele DataFrame count voor vergelijking (hoewel een table count ook werkt)
            # Voor een grotere robuustheid, tellen we de rijen na het schrijven:
            backup_rijen_aantal = spark.table(backup_tabel).count()
            originele_rijen_aantal = df_origineel.count() 

            if backup_rijen_aantal != originele_rijen_aantal:
                raise ValueError(f"Back-up tabel {backup_tabel} heeft niet hetzelfde aantal rijen ({backup_rijen_aantal}) als de originele tabel ({originele_rijen_aantal}).")

            print(f"Back-up tabel succesvol aangemaakt en gevuld met {backup_rijen_aantal} rijen.")

            # 5. Verwijder de oude NIEUWE tabelstructuur
            spark.sql(f"DROP TABLE IF EXISTS {nieuwe_tabel}")

            if sid_kolom_naam:
                # --- IDENTITY KOLOM PAD: DDL + INSERT ---
                
                # 5a. Bepaal het schema van de data kolommen
                # CRUCIALE WIJZIGING 1: Voeg backticks toe rond de veldnamen voor DDL.
                data_kolom_definities = ", ".join(
                    f"`{field.name}` {field.dataType.simpleString()}" 
                    for field in df_gefilterd.schema.fields
                )
                
                # 5b. Definieer de Identity kolom en het volledige DDL
                identity_def = f"`{sid_kolom_naam}` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)"
                
                full_ddl = f"""
                    CREATE TABLE {nieuwe_tabel} (
                        {identity_def}, {data_kolom_definities}
                    )
                    USING DELTA
                """
                # De fout treedt hier op als de kolomnamen in data_kolom_definities niet gequote zijn.
                spark.sql(full_ddl)
                print(f"Nieuwe tabelstructuur met IDENTITY-kolom aangemaakt: {nieuwe_tabel}")

                # 5c. Voer de data-insert uit vanuit de back-up
                # CRUCIALE WIJZIGING 2: Voeg backticks toe rond de kolomnamen voor DML.
                kolommen_invoegen_gequote = ", ".join(f"`{k}`" for k in kolommen_behouden)
                
                # De insert vult alleen de datavelden, de sid_kolom wordt automatisch gegenereerd
                insert_sql = f"INSERT INTO {nieuwe_tabel} ({kolommen_invoegen_gequote}) SELECT {kolommen_invoegen_gequote} FROM {backup_tabel}"
                # De fout treedt ook hier op als de kolomnamen in kolommen_invoegen_gequote niet gequote zijn.
                spark.sql(insert_sql)
                
                print(f"Data succesvol ingevoegd in {nieuwe_tabel}, IDENTITY-kolom automatisch gevuld.")

            else:
                # --- GEEN IDENTITY KOLOM PAD: saveAsTable (Oorspronkelijke methode) ---
                
                # saveAsTable zal de nieuwe tabel AANMAKEN met het correcte, gefilterde schema.
                df_gefilterd.write.format("delta").mode("overwrite").saveAsTable(nieuwe_tabel)
                print(f"Nieuwe tabel succesvol aangemaakt en gevuld met saveAsTable: {nieuwe_tabel}")
            
            # Count checks (nu voor beide paden)
            nieuwe_rijen_aantal = spark.table(nieuwe_tabel).count()

            if nieuwe_rijen_aantal != backup_rijen_aantal:
                raise ValueError(f"Nieuwe tabel {nieuwe_tabel} heeft na de schrijfactie een onjuist aantal rijen ({nieuwe_rijen_aantal}). Back-up is beschikbaar via {backup_tabel}.")

            # --- STAPPEN 6, 7, 8: VERVANGING EN OPSCHONING ---

            # 6. Verwijder de ORIGINELE tabel
            spark.sql(f"DROP TABLE IF EXISTS {originele_tabel}")
            print(f"Originele tabel definitief verwijderd: {originele_tabel}")
            
            # 7. Hernoem de NIEUWE tabel naar de ORIGINELE naam
            spark.sql(f"ALTER TABLE {nieuwe_tabel} RENAME TO {originele_tabel}")
            print(f"Tabel {nieuwe_tabel} hernoemd naar {originele_tabel}. Migratie voltooid.")

            # 8. Verwijder de back-up tabel
            spark.sql(f"DROP TABLE IF EXISTS {backup_tabel}")
            print(f"Back-up tabel verwijderd: {backup_tabel}")
            
        except Exception as e:
            # Foutafhandeling
            print(f"!!! FOUT opgetreden bij verwerking van tabel {originele_tabel}: {e}")
            print(f"Belangrijk: De back-up tabel {backup_tabel} is BEWAARD en bevat de data ZONDER de te verwijderen kolommen. Handmatig herstel is mogelijk.")
            # We gooien de error opnieuw, zodat de job stopt en de error zichtbaar is.
            raise

    print("--- Alle opgegeven kolommen zijn succesvol verwijderd, tabellen zijn vervangen en Identity-kolommen zijn bijgewerkt. ---")

#
# 8 PAS KOLOMVOLGORDE AAN
#


def pas_kolomvolgorde_aan(spark, catalog, target_schema, kolom_volgorde_mapping):
    """
    Past de volgorde van kolommen in Delta-tabellen aan op basis van een opgegeven mapping.

    Parameters:
    - spark: De SparkSession (vereist in Databricks).
    - catalog: De naam van de catalogus.
    - target_schema: De naam van het doel-schema.
    - kolom_volgorde_mapping: Een dictionary waarin de sleutels de tabelnamen zijn
      en de waarden lijsten zijn met de gewenste volgorde van kolommen.
    """
    for tabel, nieuwe_volgorde in kolom_volgorde_mapping.items():
        # Definieer de volledige namen van de tabellen
        oude_tabel_pad = f"{catalog}.{target_schema}.{tabel}"
        nieuwe_tabel_pad = f"{oude_tabel_pad}_temp_nieuwe_volgorde"

        print(f"Start verwerking voor tabel: {oude_tabel_pad}")

        try:
            # Lees de data van de originele tabel in een DataFrame
            df = spark.table(oude_tabel_pad)
            originele_kolommen = df.columns

            # Controleer of alle kolommen in de nieuwe volgorde bestaan in de originele tabel
            for kolom in nieuwe_volgorde:
                if kolom not in originele_kolommen:
                    raise ValueError(f"Kolom '{kolom}' bestaat niet in tabel '{tabel}'.")

            # Controleer op ontbrekende kolommen (exclusief SID-kolommen)
            ontbrekende_kolommen = [
                kolom for kolom in originele_kolommen
                if not kolom.lower().startswith("sid_") and kolom not in nieuwe_volgorde
            ]
            if ontbrekende_kolommen:
                raise ValueError(
                    f"De volgende kolommen ontbreken in de nieuwe volgorde voor tabel '{tabel}': {ontbrekende_kolommen}"
                )
            
            # 1. Bepaal de kolommen voor dataoverdracht (exclusief SID-kolommen)
            kolommen_om_te_selecteren = [
                kolom for kolom in nieuwe_volgorde
            ]
            
            # 2. Creëer een nieuw DataFrame met de correcte kolomvolgorde
            nieuwe_df = df.select(*kolommen_om_te_selecteren)
            
            print(f"Schrijft data naar tijdelijke tabel: {nieuwe_tabel_pad}")
            nieuwe_df.write.format("delta").mode("overwrite").saveAsTable(nieuwe_tabel_pad)

            # 3. Controleer of het aantal rijen overeenkomt
            originele_rijen_aantal = spark.table(oude_tabel_pad).count()
            nieuwe_rijen_aantal = spark.table(nieuwe_tabel_pad).count()
            if nieuwe_rijen_aantal != originele_rijen_aantal:
                raise ValueError(f"Aantal rijen in de nieuwe tabel ({nieuwe_rijen_aantal}) komt niet overeen met het aantal rijen in de originele tabel ({originele_rijen_aantal}).")

            print(f"Data succesvol overgezet naar tijdelijke tabel: {nieuwe_tabel_pad}")

            # 4. Verwijder de originele tabel en hernoem de tijdelijke
            print(f"Verwijdert originele tabel {oude_tabel_pad}...")
            spark.sql(f"DROP TABLE IF EXISTS {oude_tabel_pad}")

            print(f"Hernoemt {nieuwe_tabel_pad} naar {oude_tabel_pad}...")
            spark.sql(f"ALTER TABLE {nieuwe_tabel_pad} RENAME TO {oude_tabel_pad}")
            
            # 5. Vernieuw de metadata om de wijziging zichtbaar te maken
            print(f"Vernieuwt de catalogus metadata voor tabel {oude_tabel_pad}...")
            spark.catalog.refreshTable(oude_tabel_pad)

            print(f"Tabel {oude_tabel_pad} succesvol aangepast met nieuwe kolomvolgorde.")
            print("-" * 50)

        except Exception as e:
            print(f"Fout opgetreden bij verwerking van tabel {oude_tabel_pad}: {e}")
            raise

    print("De kolomvolgorde is succesvol aangepast voor alle opgegeven tabellen.")

#
# 9 UPDATE TABLE FROM OTHER TABLE
#


def update_table_from_other_table(spark, CATALOG, TARGET_SCHEMA, mapping):
    """
    Voert dynamische MERGE-updates uit in een Databricks Delta Lake-omgeving.
    """
    for map_item in mapping["mappings"]:
        source_table = map_item["source_table"]
        target_table = map_item["target_table"]
        source_fields = map_item["source_fields"]
        target_fields = map_item["target_fields"]
        id_field = map_item["id_field"]
        conditions = map_item.get("conditions", "1=1") 
        
        # 1. Definieer de SET-clausule (voor MERGE)
        # De syntax voor SET is eenvoudiger in MERGE (bijv. target.veld = source.veld)
        set_assignments = ", ".join(
            f"target.{target_field} = source.{source_field}"
            for source_field, target_field in zip(source_fields, target_fields)
        )
        
        # 2. Definieer de ON-clausule (de join-conditie)
        join_condition = f"target.{id_field} = source.{id_field}"
        
        # 3. Bouw de SQL-query met MERGE INTO
        query = f"""
        MERGE INTO {CATALOG}.{TARGET_SCHEMA}.{target_table} AS target
        USING {CATALOG}.{TARGET_SCHEMA}.{source_table} AS source
        ON {join_condition}
        WHEN MATCHED AND {conditions} THEN
            UPDATE SET {set_assignments}
        """
        
        # Voer de query uit
        spark.sql(query)
        print(f"Tabel {target_table} is geupdate met data van tabel {source_table}.")