# Databricks notebook source
# def bepaal_veranderde_records(huidig_df: DataFrame, nieuw_df: DataFrame, business_key: str, naam_id: str, uitzonderings_kolommen: list):
#     """
#     Identificeert en retourneert unieke rij-identificatoren van gewijzigde records tussen twee DataFrames op basis van een business_key en verschillende kolommen.

#     Parameters:
#         huidig_df (DataFrame): Het huidige DataFrame met de bestaande gegevens.
#         nieuw_df (DataFrame): Het nieuwe DataFrame met bijgewerkte gegevens.
#         business_key (str): De naam van de kolom die als business_key wordt gebruikt om records te identificeren.

#     Returns:
#         Vier DataFrames: Veranderde, verwijderde, opnieuw ingevoerde en nieuwe records.
#     Laatste update: 22-02-2024
#     """

#     # Pas huidig DataFrame aan voor de vergelijking
#     huidig_df_temp = huidig_df.drop(*uitzonderings_kolommen)
#     nieuw_df_temp = nieuw_df.drop(*uitzonderings_kolommen)
    
#     # Identificeer gewijzigde records
#     df_veranderd = (nieuw_df_temp.subtract(huidig_df.filter(column("mtd_actie") != "deleted").filter(col(naam_id)!=0).drop(*uitzonderings_kolommen)).select(business_key))

#     # Identificeer verwijderde records
#     df_verwijderd = (huidig_df.filter(column(naam_id) != 0).filter(column("mtd_actie") != "deleted").drop(*uitzonderings_kolommen)
#                     .join(nieuw_df_temp, [business_key], "left_anti").select(business_key)) # wel in rechts, niet in links
                    
#     # Identificeer opnieuw ingvulde records
#     df_opnieuw_ingevoerd = (huidig_df.filter(col("mtd_actie") == "deleted").drop(*uitzonderings_kolommen)
#             .join(nieuw_df, business_key, 'inner') #records in B not in A 
#             .select(business_key))
    
#     # Identificeer ingevoegde records
#     df_ingevoegd = nieuw_df_temp.join(huidig_df_temp, [business_key], "left_anti").select(business_key)
    
#     # Filter de veranderde set door reinserted en ingevoegde records te verwijderen
#     df_verandert_filter = df_veranderd.subtract(df_opnieuw_ingevoerd)
#     df_verandert_filter = df_verandert_filter.subtract(df_ingevoegd)
#     df_verandert_filter = df_verandert_filter.subtract(df_verwijderd)

#     return df_verandert_filter, df_verwijderd, df_opnieuw_ingevoerd, df_ingevoegd

# def initialiseer_historisering(df: DataFrame, schema_catalog: str, business_key: str, naam_nieuw_df: str,
#                                 naam_id: str, naam_bk: str, overwrite_schema: bool, uitzonderings_kolommen: list):
#     """
#     Bereid gegevens voor op historisering door historische data te markeren met relevante metadata.

#     Parameters:
#         - df (PySpark DataFrame): Het huidige DataFrame.
#         - schema_catalog (str): Naam van het schema waar de tabel moet worden opgeslagen.
#         - business_key (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
#         - naam_nieuw_df (str): Naam van het nieuwe DataFrame in de catalogus.
#         - naam_id (str): Naam van de id (surrogaat_sleutel/pk)
#         - naam_bk (str): Naam van de business_key (bk)
#         - overwrite_schema (bool): Met deze boolean kun je aangeven of het schema van de tabel overschreven mag worden.
#                                         - True -> mag overschreven worden
#                                         - False -> mag NIET overschreven worden (default) 
#         - uitzonderings_kolommen (list): Lijst met welke kolommen uitgesloten moeten worden voor het vergelijken van de versies.

#     Laatste update: 06-02-2023
#     """
#     # Controleer of het nieuwe DataFrame de vereiste kolommen bevat
#     vereiste_kolommen = ['mtd_geldig_van', 'mtd_geldig_tot', naam_bk, 'mtd_record_actief', 'mtd_actie', naam_id]
#     ontbrekende_kolommen = [kolom for kolom in vereiste_kolommen if kolom not in df.columns]

#     # Controleer of de opgegeven identifier uniek is
#     controle_unieke_waarden_kolom(df=df, kolom=business_key)

#     # Check if the column contains only null values
#     all_null_count = df.filter(df[business_key].isNull()).count()

#     if all_null_count == df.count():
#         sys.exit(f"The column '{business_key}' is filled with only null values.")
        
#     if ontbrekende_kolommen:      
#         # Roep de functie tijdzone_amsterdam aan om de correcte tijdsindeling te krijgen
#         huidige_datum_tz = tijdzone_amsterdam()

#         # Werk de einddatum, record_actief en begindatum kolommen bij in het huidige DataFrame
#         print(f"Ontbrekende kolommen: {', '.join(ontbrekende_kolommen)}.\nDeze worden aan het DataFrame toegevoegd...")
#         temp_df = (df.withColumn("mtd_geldig_van", huidige_datum_tz) 
#                         .withColumn("mtd_geldig_tot", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss")) 
#                         .withColumn("mtd_record_actief", lit(True)) 
#                         .withColumn("mtd_actie", lit("inserted"))
#                         .withColumn(naam_bk, column(business_key).cast("string")))
        
#         temp_df = voeg_willekeurig_toe_en_hash_toe(df=temp_df, business_key=business_key, pk=naam_id)
        
#         # Controleer of de opgegeven identifier uniek is
#         controle_unieke_waarden_kolom(df=temp_df, kolom=naam_id)
        
#         # Bepaal de juiste volgorde van de kolommen
#         output_volgorde, _ = bepaal_kolom_volgorde(df = temp_df, gewenste_kolom_volgorde = uitzonderings_kolommen)
#         output_volgorde = maak_onbekende_dimensie(df=output_volgorde, naam_id=naam_id, naam_nieuw_df=naam_nieuw_df, naam_bk=naam_bk, uitzonderings_kolommen=uitzonderings_kolommen)

#         # Sla de gegevens op in delta-formaat in het opgegeven schema
#         print(f"De tabel wordt nu opgeslagen in {schema_catalog_print} onder de naam: {naam_nieuw_df}.")
#         output_volgorde.write.saveAsTable(f'{schema_catalog}.{naam_nieuw_df}', mode='overwrite', overwriteSchema=overwrite_schema)    
#     return
    
# def updaten_historisering_dwh(nieuw_df: DataFrame, schema_catalog: str, business_key: str,  naam_nieuw_df: str, 
#                               naam_id: str, naam_bk: str, overwrite_schema: bool, uitzonderings_kolommen: list, huidig_dwh: str = None):
#     """
#     Voegt historische gegevens toe aan een PySpark DataFrame om wijzigingen bij te houden.

#     Args:
#         nieuw_df (str of object): Naam van het nieuwe DataFrame dat verwijst naar een temporary view met gewijzigde gegeven (str)
#                                            met de suffix '_temp_view' erachter of een Python DataFrame
#         schema_catalog (str): Naam van het schema en catalog waar de tabel instaat of opgeslagen moet worden. 
#                                 Bijvoorbeeld: {catalog.schema} = "dpms_dev.silver"
#         business_key (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
#         naam_nieuw_df (str): Naam van DataFrame/Tabel zoals die opgeslagen is / moet worden in de opgegeven catalog/schema
#         naam_id (str): Naam van de primary key (surrogaat_sleutel)
#         naam_bk (str): Naam van de business_key (gerelateerd aan business_key)
#         overwrite_schema (bool): Met deze boolean kun je aangeven of het schema van de tabel overschreven mag worden.
#                                  - True -> mag overschreven worden
#                                  - False -> mag NIET overschreven worden (default) 
#         uitzonderings_kolommen (list): Lijst met welke kolommen uitgesloten moeten worden voor het vergelijken van de versies.
#         huidig_dwh (str, optioneel): Naam van het huidige DWH DataFrame. Indien niet opgegeven, wordt
#                                      de huidige tabelnaam van 'nieuw_df' gebruikt (komt overeen met het DWH).
#     """ 

#     # Lees de huidige versie van de opgegeven tabel in
#     if huidig_dwh is None:
#         huidig_dwh_tabel = spark.read.table(f"{schema_catalog}.{naam_nieuw_df}").cache()
#     else:
#         huidig_dwh_tabel = spark.read.table(f"{schema_catalog}.{huidig_dwh}").cache()

#     # Roep de functie tijdzone_amsterdam aan om de correcte tijdsindeling te krijgen
#     huidige_datum_tz = tijdzone_amsterdam() 
    
#     # vul lege waardes in als het een dimensie tabel betreft
#     if "_d_" in naam_nieuw_df:
#         nieuw_df = vul_lege_cellen_in(df=nieuw_df, uitzonderings_kolommen=uitzonderings_kolommen) 

#     # Bepaal de volgorde van de kolommen
#     _, volgorde_kolommen = bepaal_kolom_volgorde(df = huidig_dwh_tabel, gewenste_kolom_volgorde = uitzonderings_kolommen)

#     # Roep de functie bepaal_veranderde_records aan om de records te krijgen van rijen die veranderd zijn
#     unieke_df_id_verandert, unieke_df_id_verwijdert, unieke_df_id_opnieuw_ingevoegd, unieke_df_id_toegevoegd = bepaal_veranderde_records(huidig_df=huidig_dwh_tabel, nieuw_df=nieuw_df, business_key=business_key, uitzonderings_kolommen=uitzonderings_kolommen, naam_id=naam_id) 
    
#     # Check of er een wijziging is
#     if all(df.isEmpty() for df in [unieke_df_id_verandert, unieke_df_id_verwijdert, unieke_df_id_opnieuw_ingevoegd, unieke_df_id_toegevoegd]):
#         # Sla de gegevens op in delta-formaat in het opgegeven schema
#         print(f"Er zijn geen wijzigingen constateert in vergelijking met de vorige versie. De tabel wordt nu opgeslagen in {schema_catalog_print} onder de naam: {naam_nieuw_df}")
#         return
    
#     else:
#         print("Er zijn wel wijzigingen constateert in vergelijking met de vorige versie.")

#         # Voeg alle losse dataframes samen om later te broadcasten
#         df_bk_samengevoegd_1 = (unieke_df_id_verwijdert.union(unieke_df_id_verandert))
#         df_bk_samengevoegd_2 = (unieke_df_id_verandert.union(unieke_df_id_opnieuw_ingevoegd).union(unieke_df_id_toegevoegd))

#         # Dataframes opsplitsen in delen die wel aangepast moeten worden en ander deel niet.
#         # Dit doen wij voor de efficiëntie en optimalisatie van de functie
#         # Aangezien we met de id's filteren kan het contra-intuïtief zijn om bij geen_aanpassing een left_anti join te gebruiken
#         huidig_dwh_tabel_geen_aanpassing = (huidig_dwh_tabel.join(broadcast(df_bk_samengevoegd_1), business_key, "left_anti").select(*volgorde_kolommen))
#         huidig_dwh_tabel_wel_aanpassing = (huidig_dwh_tabel.join(broadcast(df_bk_samengevoegd_1), business_key).select(*volgorde_kolommen))

#         # Maak een opsplitsing voor de verschillende stappen
#         huidig_dwh_verwijderd = (huidig_dwh_tabel_wel_aanpassing.join(unieke_df_id_verwijdert, business_key, "inner")
#                                 .withColumn("mtd_actie", lit("deleted")))
#         huidig_dwh_verandert = (huidig_dwh_tabel_wel_aanpassing.join(unieke_df_id_verandert, business_key, "inner"))

#         # Voeg ze weer samen
#         huidig_dwh_tabel_wel_aanpassing = (huidig_dwh_verwijderd.union(huidig_dwh_verandert))

#         # Pas de records in het DWH (huidige tabel) aan die veranderd zijn
#         temp_huidig_dwh_tabel = (huidig_dwh_tabel_wel_aanpassing
#                                 .withColumn("nieuwe_geldig_tot", huidige_datum_tz)
#                                 .withColumn("record_actief_update", lit(False))
#                                 .drop("mtd_geldig_tot").withColumnRenamed("nieuwe_geldig_tot", "mtd_geldig_tot")
#                                 .drop("mtd_record_actief").withColumnRenamed("record_actief_update", "mtd_record_actief")
#                                 .select(*volgorde_kolommen))

#         # Voeg de 2 delen van het DWH weer samen
#         rejoined_huidig_dwh_tabel = (huidig_dwh_tabel_geen_aanpassing
#                                     .union(temp_huidig_dwh_tabel).select(*volgorde_kolommen))
                        
#         # Dataframes filteren op de id's die aangepast zijn en met de nieuwe gegevens weer toegevoegd worden aan de DWH 
#         # 'Broadcast' van business_keys voor join speed_optimalisatie 
#         temp_nieuw_df_wel_aanpassing = nieuw_df.join(broadcast(df_bk_samengevoegd_2), business_key) 

#         # Maak voor iedere individuele actie een tabel en pas de actie kolom aan 
#         df_toegevoegd = (temp_nieuw_df_wel_aanpassing.join(unieke_df_id_toegevoegd, business_key, "inner").withColumn("mtd_actie", lit("inserted")))
#         df_opnieuw_ingevoegd = (temp_nieuw_df_wel_aanpassing.join(unieke_df_id_opnieuw_ingevoegd, business_key, "inner")
#                                 .withColumn("mtd_actie", lit("reinserted")))
#         df_verandert = (temp_nieuw_df_wel_aanpassing.join(unieke_df_id_verandert, business_key, "inner").withColumn("mtd_actie", lit("changed")))

#         # voeg dataframes weer samen en pas historiseringskolommen aan
#         df_nieuwe_tabel_samengevoegd = (df_toegevoegd.union(df_opnieuw_ingevoegd).union(df_verandert)
#                                         .withColumn(naam_id, lit(None))
#                                         .withColumn(naam_bk, col(business_key).cast("string"))
#                                         .withColumn("mtd_record_actief", lit(True))
#                                         .withColumn("mtd_geldig_van", huidige_datum_tz)
#                                         .withColumn("mtd_geldig_tot", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss"))
#                                         .select(*volgorde_kolommen))

#         df_nieuwe_tabel_samengevoegd_ss = voeg_willekeurig_toe_en_hash_toe(df=df_nieuwe_tabel_samengevoegd, business_key=business_key, pk=naam_id)
        
#         # Sorteer beide dataframe op kolommen, zodat ze samengevoegd kunnen worden
#         output = rejoined_huidig_dwh_tabel.union(df_nieuwe_tabel_samengevoegd_ss)

#         # Controleer of alle surrogaat_sleutels in de tabel uniek zijn ## Kan verbeterd worden
#         #controle_unieke_waarden_kolom(df=output, kolom=naam_id)
    
#         # Sla de gegevens op in delta-formaat in het opgegeven schema
#         print(f"De tabel wordt nu opgeslagen in {schema_catalog_print} onder de naam: {naam_nieuw_df}")
#         output.write.saveAsTable(f'{schema_catalog}.{naam_nieuw_df}', mode='overwrite', overwriteSchema=overwrite_schema) # is langzaam (single node)
#         return

# def toepassen_historisering(bestaande_tabel, schema_catalog: str, naam_tabel: str, business_key: str, naam_bk: str, 
#                             naam_id: str, uitzonderings_kolommen: list = [], huidig_dwh: str = None, overwrite_schema: bool = False):
#     """
#     Deze regisseurfunctie roept op basis van bepaalde criteria andere functies aan en heeft hiermee de controle over de uitvoering van het historiseringsproces.

#     Deze functie gaat ervan uit dat je een string opgeeft die verwijst naar een SQL temporary view of Python DataFrame. Wanneer jij bij bestaande_tabel een Python DataFrame opgeeft, moet je verplicht naam_tabel invullen. Aangezien Python geen objectnaam kan afleiden van objecten.
    
#     Args:
#         bestaande_tabel (str of object): Naam van het nieuwe DataFrame dat verwijst naar een temporary view met gewijzigde gegeven (str)
#                                            met de suffix '_temp_view' erachter of een Python DataFrame
#         schema_catalog (str): Naam van het schema en catalog waar de tabel instaat of opgeslagen moet worden. 
#                                 Bijvoorbeeld: {catalog.schema} = "dpms_dev.silver"
#         naam_tabel (str): Naam van DataFrame/Tabel zoals die opgeslagen is / moet worden in de opgegeven catalog/schema
#         business_key (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
#         naam_bk (str): Naam van de business_key (gerelateerd aan business_key)
#         naam_id (str): Naam van de primary key (surrogaat_sleutel)
#         uitzonderings_kolommen (list): Lijst met welke kolommen uitgesloten moeten worden voor het vergelijken van de versies.
#         huidig_dwh (str, optioneel): Naam van het huidige DWH DataFrame. Indien niet opgegeven, wordt
#                                      de huidige tabelnaam van 'nieuw_df' gebruikt (komt overeen met het DWH).
#         overwrite_schema (bool): Met deze boolean kun je aangeven of het schema van de tabel overschreven mag worden.
#                                  - True -> mag overschreven worden
#                                  - False -> mag NIET overschreven worden (default) 
        
#     Laatste update: 06-02-2024

#     Raises:
#         ValueError: Als je bij de parameter naam_id en naam_bk geen 'mtd_' als prefix gebruikt, klapt hij eruit

#         ValueError: Als de tabel/dataframe-naam niet kan worden afgeleid vanuit het object. 
#                     Indien je bij bestaande_tabel een Python DataFrame meegeeft, moet je de naam van de tabel geven 
#                     zoals bij naam_tabel.
#     """
#     global standaard_uitzondering_kolommen
#     standaard_uitzondering_kolommen = [naam_bk, naam_id, "mtd_geldig_van", "mtd_geldig_tot", "mtd_record_actief", "mtd_actie"]
#     uitzonderings_kolommen = standaard_uitzondering_kolommen + uitzonderings_kolommen
#     global schema_catalog_print
#     schema_catalog_print = "-".join(schema_catalog)
    
#     # Check voor invoer correcte waarde
#     if "mtd_" not in naam_id or "mtd_" not in naam_bk:
#         raise ValueError("Aangezien het om meta (mtd) kolommen gaat, graag de volgende prefix invoeren bij de kolommen 'naam_id' en 'naam_bk': mtd_ ")

#     # Bepaal of de input een SQL temporary view is of een PySpark dataframe.
#     if type(bestaande_tabel) == str:
#         if "_temp_view" in bestaande_tabel:
#             naam_bestaande_tabel = bestaande_tabel.replace("_temp_view", "")
#             temp_bestaande_tabel = spark.table(naam_bestaande_tabel) # zorgt niet voor verbeteringen: .cache()
#         else:
#             raise ValueError("De temporary view is niet voorzien van de juist suffix: '_temp_view'")
        
#     elif type(bestaande_tabel) != str and (naam_tabel is not None and naam_tabel != ""):
#         temp_bestaande_tabel = bestaande_tabel # zorgt niet voor verbeteringen: .cache()
#     else:
#          raise ValueError("Python kan de tabel/dataframe-naam niet afleiden vanuit het object. Indien je bij nieuw_df een Python DataFrame meegeeft, moet je de naam van de tabel geven zoals bij naam_tabel")

#     # Haal metadata op uit de Unity Catalog
#     tabellen_catalog = spark.sql(f"SHOW TABLES IN {schema_catalog}")

#     # Maak een set van alle tabellen in het opgegeven schema
#     set_tabellen_catalog = {row["tableName"] for row in tabellen_catalog.collect()}

#     # Controleer of de opgegeven tabel al bestaat in het opgegeven schema
#     if naam_tabel in set_tabellen_catalog:
#         print(f"De tabel: {naam_tabel} bevindt zich in de Unity Catalogus onder het volgende schema: {schema_catalog_print}")
#         updaten_historisering_dwh(nieuw_df=temp_bestaande_tabel, schema_catalog=schema_catalog, business_key=business_key, naam_nieuw_df=naam_tabel, naam_id=naam_id, naam_bk=naam_bk, uitzonderings_kolommen=uitzonderings_kolommen, overwrite_schema=overwrite_schema)
#     else:
#         print(f"Dit is de eerste keer dat je de tabel: {naam_tabel} wilt historiseren. Historisering wordt nu toegepast...")
#         initialiseer_historisering(df=temp_bestaande_tabel, schema_catalog=schema_catalog, business_key=business_key, naam_nieuw_df=naam_tabel, naam_id=naam_id, naam_bk=naam_bk, uitzonderings_kolommen=uitzonderings_kolommen, overwrite_schema=overwrite_schema)
#     return "Einde functie"
