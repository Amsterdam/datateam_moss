# Inladen packages
import sys
import time
import uuid
import hashlib
import xxhash
import random
import string

from databricks.sdk.runtime import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.window import Window

def voeg_willekeurig_toe_en_hash_toe(df: DataFrame, business_key: str, pk: str):
    """
    Neemt de naam van een kolom als invoer aan, veronderstelt dat het een gehashte waarde bevat,
    voegt aan elke waarde een willekeurig woord of getal toe en maakt een nieuwe hash.

    Parameters:
    - df: DataFrame: Het DataFrame waar je de hash aan wil toevoegen
    - business_key: str: De naam van de invoerkolom.
    - pk: str: De naam van de primary key

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
        return nieuwe_hash_waarde

    # Registreer de UDF
    udf_voeg_willekeurig_toe_en_hash_toe = udf(voeg_willekeurig_toe_en_hash_toe_udf, returnType=StringType())
    
    # Pas de UDF toe op het DataFrame
    resultaat_df = df.withColumn(pk, udf_voeg_willekeurig_toe_en_hash_toe(col(business_key)))
    return resultaat_df

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

def tijdzone_amsterdam():
    """
    Pakt de huidige tijd en convert het naar het volgende tijdsformat yyyy-MM-dd HH:mm:ss en Amsterdamse tijdzone
    Laatste update: 04-12-2023
    """
    # Bepaal de datum van de wijziging
    huidige_datum = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Zet de string om naar een tijdstempel
    timestamp_expr = to_timestamp(lit(huidige_datum), "yyyy-MM-dd HH:mm:ss")

    # Stel de tijdzone in op "Europe/Amsterdam"
    huidige_datum_tz = from_utc_timestamp(timestamp_expr, "Europe/Amsterdam")

    return huidige_datum_tz

def bepaal_kolom_volgorde(df: DataFrame, gewenste_kolom_volgorde: list) -> DataFrame: 
    """
    Bepaalt de volgorde van kolommen in een DataFrame op basis van de opgegeven gewenste volgorde.

    Parameters:
    - df (DataFrame): Het invoer DataFrame waarvan de kolomvolgorde moet worden aangepast.
    - gewenste_kolom_volgorde (list): Een lijst met kolomnamen in de gewenste volgorde.

    Returns:
    - DataFrame: Een nieuw DataFrame met kolommen in de gespecificeerde volgorde.
    Laatste update: 09-01-2023
    """
    # Bepaal de juiste volgorde van de kolommen
    df_kolommen = df.columns
    
    for column in df_kolommen:
        if column not in gewenste_kolom_volgorde:
            column = column.lower()
            gewenste_kolom_volgorde.append(column)

    output_df = df.select(*gewenste_kolom_volgorde)
    return output_df, gewenste_kolom_volgorde

def bepaal_veranderde_records(huidig_df: DataFrame, nieuw_df: DataFrame, business_key: str):
    """
    Identificeert en retourneert unieke rij-identificatoren van gewijzigde records tussen twee DataFrames op basis van een business_key en verschillende kolommen.

    Parameters:
        huidig_df (DataFrame): Het huidige DataFrame met de bestaande gegevens.
        nieuw_df (DataFrame): Het nieuwe DataFrame met bijgewerkte gegevens.
        business_key (str): De naam van de kolom die als business_key wordt gebruikt om records te identificeren.

    Returns:
        vier dataframes
    Laatste update: 09-01-2023
    """
    
    # Pas huidig DataFrame aan voor de vergelijking
    uitzondering_kolom = ["geldig_van", "geldig_tot", "surrogaat_sleutel", "record_actief", "actie"]
    huidig_df_temp = huidig_df.drop(*uitzondering_kolom)
    nieuw_df_temp = nieuw_df.drop(*uitzondering_kolom)
    
    # Maak sets van de huidige en nieuwe business_keys
    huidige_bk_df = huidig_df_temp.select(business_key).distinct()
    nieuw_bk_df = nieuw_df_temp.select(business_key).distinct()

    # Identificeer gewijzigde records
    df_veranderd = nieuw_df_temp.subtract(huidig_df_temp).select(business_key).distinct()
    
    # Identificeer verwijderde records
    df_verwijderd = (huidig_df.join(huidige_bk_df, business_key, 'left_outer') #records in A not in B
                     .join(nieuw_bk_df, business_key, 'left_anti') #records in B not in A 
                     .filter(col("actie") != "deleted").select(business_key).drop(*uitzondering_kolom))

    # Identificeer opnieuw ingevulde records
    df_opnieuw_ingevoerd = (huidig_df.filter(col("actie") == "deleted").drop(*uitzondering_kolom)
            .join(nieuw_bk_df, business_key, 'inner') #records in B not in A 
            .select(business_key).drop(*uitzondering_kolom))
    
    # Identificeer ingevoegde records
    df_ingevoegd = (df_veranderd.join(df_veranderd, business_key, "left_outer") #records in A not in B
                    .join(huidige_bk_df, business_key, "left_anti") #records in B not in A 
                    .select(business_key))

    # Filter de veranderde set door reinserted en ingevoegde records te verwijderen
    df_verandert_filter = df_veranderd.subtract(df_opnieuw_ingevoerd).subtract(df_ingevoegd)

    return df_verandert_filter, df_verwijderd, df_opnieuw_ingevoerd, df_ingevoegd

def initialiseer_historisering(df: DataFrame, schema_catalog: str, business_key: str, naam_nieuw_df: str, naam_id: str, naam_bk: str):
    """
    Bereid gegevens voor op historisering door historische data te markeren met relevante metadata.

    Parameters:
        - df (DataFrame): Het huidige DataFrame.
        - schema_catalog (str): Naam van het schema waar de tabel moet worden opgeslagen.
        - business_key (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
        - naam_nieuw_df (str): Naam van het nieuwe DataFrame in de catalogus.
        - naam_id (str): Naam van de id (surrogaat_sleutel/pk)
        - naam_bk (str): Naam van de business_key (bk)

    Laatste update: 26-01-2023
    """
    # Controleer of het nieuwe DataFrame de vereiste kolommen bevat
    vereiste_kolommen = ['mtd_geldig_van', 'mtd_geldig_tot', naam_bk, 'mtd_record_actief', 'mtd_actie']
    ontbrekende_kolommen = [kolom for kolom in vereiste_kolommen if kolom not in df.columns]

    # Controleer of de opgegeven identifier uniek is
    controle_unieke_waarden_kolom(df=df, kolom=business_key)
    
    # Als het een dimensietable betreft een record aanmaken voor missende waarden
    if "_d_" in naam_nieuw_df:
        c
         Create a new list of lists with concatenated strings
new_data = [[f"{col} heeft geen waarde" for col in columns]]

    if ontbrekende_kolommen:      
        # Roep de functie tijdzone_amsterdam aan om de correcte tijdsindeling te krijgen
        huidige_datum_tz = tijdzone_amsterdam()

        # Werk de einddatum, record_actief en begindatum kolommen bij in het huidige DataFrame
        print(f"Ontbrekende kolommen: {', '.join(ontbrekende_kolommen)}. Deze worden aan het DataFrame toegevoegd...")

        temp_df = (df.withColumn("mtd_geldig_van", huidige_datum_tz) 
                        .withColumn("mtd_geldig_tot", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss")) 
                        .withColumn("mtd_record_actief", lit(True)) 
                        .withColumn("mtd_actie", lit("inserted"))
                        .withColumn(naam_bk, col(business_key)))
        
        temp_df = voeg_willekeurig_toe_en_hash_toe(df=temp_df, business_key=business_key, pk=naam_id)

        # Controleer of de opgegeven identifier uniek is
        controle_unieke_waarden_kolom(df=temp_df, kolom=naam_id)
        
        # Bepaal de juiste volgorde van de kolommen
        volgorde_kolommen = [naam_bk, naam_id, "mtd_record_actief", "mtd_geldig_van", "mtd_geldig_tot", "mtd_actie"]
        output_volgorde, _ = bepaal_kolom_volgorde(df = temp_df, gewenste_kolom_volgorde = volgorde_kolommen)
        output_volgorde_cached = (output_volgorde.cache()) #.cache())
        
        # Sla de gegevens op in delta-formaat in het opgegeven schema
        print(f"De tabel wordt nu opgeslagen in {schema_catalog} onder de naam: {naam_nieuw_df}.")
        output_volgorde_cached.write.saveAsTable(f'{schema_catalog}.{naam_nieuw_df}', mode='overwrite')
    
    
    return
    
def updaten_historisering_dwh(nieuw_df: DataFrame, business_key: str, schema_catalog: str, naam_nieuw_df: str, huidig_dwh: str = None):
    """
    Voegt historische gegevens toe aan een PySpark DataFrame om wijzigingen bij te houden.

    Args:
        nieuw_df (DataFrame): DataFrame met gewijzigde gegevens.
        business_key (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
        naam_nieuw_df (str): Naam van de tabel in het opgegeven schema.
        huidig_dwh (str, optioneel): Naam van het huidige DWH DataFrame. Indien niet opgegeven, wordt 
                                     de huidige tabelnaam van 'nieuw_df' gebruikt (komt overeen met het DWH).
    Laatste update: 09-01-2023
    """ 
    
    # Lees de huidige versie van de opgegeven tabel in
    if huidig_dwh is None:
        huidig_dwh_tabel = spark.read.table(f"{schema_catalog}.{naam_nieuw_df}").cache()
    else:
        huidig_dwh_tabel = spark.read.table(f"{schema_catalog}.{huidig_dwh}").cache()

    # Roep de functie tijdzone_amsterdam aan om de correcte tijdsindeling te krijgen
    huidige_datum_tz = tijdzone_amsterdam() 

    # Bepaal de juiste volgorde van de kolommen
    volgorde_kolommen_gewenst = [business_key, "record_actief", "surrogaat_sleutel", "geldig_van", "geldig_tot", "actie"]
    _, volgorde_kolommen = bepaal_kolom_volgorde(df = huidig_dwh_tabel, gewenste_kolom_volgorde = volgorde_kolommen_gewenst)
            
    # Roep de functie bepaal_veranderde_records aan om de records te krijgen van rijen die veranderd zijn
    unieke_df_id_verandert, unieke_df_id_verwijdert, unieke_df_id_opnieuw_ingevoegd, unieke_df_id_toegevoegd = bepaal_veranderde_records(huidig_dwh_tabel, nieuw_df, business_key)

    # Voeg alle losse dataframes samen om later te broadcasten
    df_bk_samengevoegd_1 = (unieke_df_id_verwijdert.union(unieke_df_id_verandert))
    df_bk_samengevoegd_2 = (unieke_df_id_verandert.union(unieke_df_id_opnieuw_ingevoegd).union(unieke_df_id_toegevoegd))

    # Dataframes opsplitsen in delen die wel aangepast moeten worden en ander deel niet.
    # Dit doen wij voor de efficiëntie en optimalisatie van de functie
    # Aangezien we met de id's filteren kan het contra-intuïtief zijn om bij geen_aanpassing een left_anti join te gebruiken
    huidig_dwh_tabel_geen_aanpassing = (huidig_dwh_tabel.join(broadcast(df_bk_samengevoegd_1), business_key, "left_anti").select(*volgorde_kolommen))
    huidig_dwh_tabel_wel_aanpassing = (huidig_dwh_tabel.join(broadcast(df_bk_samengevoegd_1), business_key).select(*volgorde_kolommen))

    # Maak een opsplitsing voor de verschillende stappen
    huidig_dwh_verwijderd = (huidig_dwh_tabel_wel_aanpassing.join(unieke_df_id_verwijdert, business_key, "inner")
                             .withColumn("actie", lit("deleted")))
    huidig_dwh_verandert = (huidig_dwh_tabel_wel_aanpassing.join(unieke_df_id_verandert, business_key, "inner"))

    # Voeg ze weer samen
    huidig_dwh_tabel_wel_aanpassing = (huidig_dwh_verwijderd.union(huidig_dwh_verandert))

    # Pas de records in het DWH (huidige tabel) aan die veranderd zijn
    temp_huidig_dwh_tabel = (huidig_dwh_tabel_wel_aanpassing
                             .withColumn("nieuwe_geldig_tot", huidige_datum_tz)
                             .withColumn("record_actief_update", lit(False))
                             .drop("geldig_tot").withColumnRenamed("nieuwe_geldig_tot", "geldig_tot")
                             .drop("record_actief").withColumnRenamed("record_actief_update", "record_actief")
                             .select(*volgorde_kolommen))
    
    # Voeg de 2 delen van het DWH weer samen
    rejoined_huidig_dwh_tabel = (huidig_dwh_tabel_geen_aanpassing
                                 .union(temp_huidig_dwh_tabel).select(*volgorde_kolommen)
                                 .repartition(10).cache())
                       
    # Dataframes filteren op de id's die aangepast zijn en met de nieuwe gegevens weer toegevoegd worden aan de DWH 
    # 'Broadcast' van business_keys voor join speed_optimalisatie 
    temp_nieuw_df_wel_aanpassing = nieuw_df.join(broadcast(df_bk_samengevoegd_2), "account_id") 

    # Maak voor iedere individuele actie een tabel en pas de actie kolom aan 
    df_toegevoegd = (temp_nieuw_df_wel_aanpassing.join(unieke_df_id_toegevoegd, business_key, "inner").withColumn("actie", lit("inserted")))
    df_opnieuw_ingevoegd = (temp_nieuw_df_wel_aanpassing.join(unieke_df_id_opnieuw_ingevoegd, business_key, "inner")
                            .withColumn("actie", lit("reinserted")))
    df_verandert = (temp_nieuw_df_wel_aanpassing.join(unieke_df_id_verandert, business_key, "inner").withColumn("actie", lit("changed")))
    
    # voeg dataframes weer samen en pas historiseringskolommen aan
    df_nieuwe_tabel_samengevoegd = (df_toegevoegd.union(df_opnieuw_ingevoegd).union(df_verandert) 
                                    .withColumn("surrogaat_sleutel", lit(None))
                                    .withColumn("record_actief", lit(True))
                                    .withColumn("geldig_van", huidige_datum_tz)
                                    .withColumn("geldig_tot", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss"))
                                    .select(*volgorde_kolommen) .repartition(10).cache())
    df_nieuwe_tabel_samengevoegd_ss = voeg_willekeurig_toe_en_hash_toe(df=df_nieuwe_tabel_samengevoegd, business_key=business_key)

    # Sorteer beide dataframe op kolommen, zodat ze samengevoegd kunnen worden
    output = rejoined_huidig_dwh_tabel.union(df_nieuwe_tabel_samengevoegd_ss).cache()

    # Controleer of alle surrogaat_sleutels in de tabel uniek zijn
    controle_unieke_waarden_kolom(df=output, kolom=business_key)

    # Sla de gegevens op in delta-formaat in het opgegeven schema
    print(f"De tabel wordt nu opgeslagen in {schema_catalog} onder de naam: {naam_nieuw_df}")
    output.write.saveAsTable(f'{schema_catalog}.{naam_nieuw_df}', mode='overwrite') # is langzaam (single node)
    return

def toepassen_historisering(bestaande_tabel, schema_catalog: str, naam_tabel: str, business_key: str, naam_bk: str, naam_id: str, huidig_dwh: str = None):
    """
    Deze regisseurfunctie roept op basis van bepaalde criteria andere functies aan en heeft hiermee de controle over de uitvoering van het historiseringsproces.

    Deze functie gaat ervan uit dat je een string opgeeft die verwijst naar een SQL temporary view of Python DataFrame. Wanneer jij bij bestaande_tabel een Python DataFrame opgeeft, moet je verplicht naam_tabel invullen. Aangezien Python geen objectnaam kan afleiden van objecten.
    
    Args:
        bestaande_tabel (str of object): Naam van het nieuwe DataFrame dat verwijst naar een temporary view met gewijzigde gegeven (str)
                                           met de suffix '_temp_view' erachter of een Python DataFrame
        schema_catalog (str): Naam van het schema en catalog waar de tabel instaat of opgeslagen moet worden. 
                                Bijvoorbeeld: {catalog.schema} = "dpms_dev.silver"
        naam_tabel (str): Naam van DataFrame/Tabel zoals die opgeslagen is / moet worden in de opgegeven catalog/schema
        business_key (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
        naam_bk (str): Naam van de business_key (gerelateerd aan business_key)
        naam_id (str): Naam van de primary key (surrogaat_sleutel)
        huidig_dwh (str, optioneel): Naam van het huidige DWH DataFrame. Indien niet opgegeven, wordt
                                     de huidige tabelnaam van 'nieuw_df' gebruikt (komt overeen met het DWH).
        
    Laatste update: 26-01-2024

    Raises:
        ValueError: Als de tabel/dataframe-naam niet kan worden afgeleid vanuit het object. 
                    Indien je bij bestaande_tabel een Python DataFrame meegeeft, moet je de naam van de tabel geven 
                    zoals bij naam_tabel.
    """   
    # Check voor invoer correcte waarde
    if "mtd_" not in naam_id or "mtd_" not in naam_bk:
        raise ValueError("Aangezien het om meta (mtd) kolommen gaat, graag de volgende prefix invoeren bij de naam_id en naam_bk: 'mtd_' ")

    # Bepaal of de input een SQL temporary view is of een PySpark dataframe.
    if type(bestaande_tabel) == str:
        if "_temp_view" in bestaande_tabel:
            naam_bestaande_tabel = bestaande_tabel.replace("_temp_view", "")
            temp_bestaande_tabel = spark.table(naam_bestaande_tabel).cache()
        else:
            raise ValueError("De temporary view is niet voorzien van de juist suffix: '_temp_view'")
        
    elif type(bestaande_tabel) != str and (naam_tabel is not None and naam_tabel != ""):
        temp_bestaande_tabel = bestaande_tabel.cache()
    else:
         raise ValueError("Python kan de tabel/dataframe-naam niet afleiden vanuit het object. Indien je bij nieuw_df een Python DataFrame meegeeft, moet je de naam van de tabel geven zoals bij naam_tabel")

    # Haal metadata op uit de Unity Catalog
    tabellen_catalog = spark.sql(f"SHOW TABLES IN {schema_catalog}")

    # Maak een set van alle tabellen in het opgegeven schema
    set_tabellen_catalog = {row["tableName"] for row in tabellen_catalog.collect()}

    # Controleer of de opgegeven tabel al bestaat in het opgegeven schema
    if naam_tabel in set_tabellen_catalog:
        print(f"De tabel: {naam_tabel} bevindt zich in de Unity Catalogus onder het volgende schema: {schema_catalog}")
        # updaten_historisering_dwh(nieuw_df=temp_bestaande_tabel, schema_catalog=schema_catalog, business_key=business_key, naam_nieuw_df=naam_tabel, naam_id=naam_id, naam_bk=naam_bk) 
    else:
        print(f"Dit is de eerste keer dat je de tabel: {naam_tabel} wilt historiseren. Historisering wordt nu toegepast...")
        initialiseer_historisering(df=temp_bestaande_tabel, schema_catalog=schema_catalog, business_key=business_key, naam_nieuw_df=naam_tabel, naam_id=naam_id, naam_bk=naam_bk)
    return "Historisering is toegepast!"
