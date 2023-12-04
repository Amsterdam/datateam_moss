# Databricks notebook source
# Inladen packages
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.window import Window
import time

def voeg_aangepaste_surrogaatsleutel_toe(df: DataFrame, max_bestaande_surrogaatsleutel=None):
    """
    Voeg een aangepaste surrogaatsleutelkolom toe aan een PySpark DataFrame.

    Args:
        df (DataFrame): Het oorspronkelijke DataFrame waarop de surrogaatsleutel moet worden toegevoegd.
        max_bestaande_surrogaatsleutel (int, optioneel): De maximale bestaande surrogaatsleutel in het DataFrame.
            Als niet opgegeven, wordt de surrogaatsleutel gestart vanaf 1.

    Returns:
        DataFrame: Een DataFrame met de aangepaste surrogaatsleutelkolom toegevoegd.

    Voorbeeld:
        # Gebruik van de functie om een surrogaatsleutel toe te voegen aan een DataFrame
        df = voeg_aangepaste_surrogaatsleutel_toe(df)
    Laatste update: 23-11-2023
    """
    # Creëer een window-specificatie om te sorteren op een constante waarde (0)
    window_spec = Window().orderBy(lit(0))

    # Gebruik row_number() om een unieke waarde te genereren voor elke rij, beginnend vanaf max_bestaande_surrogaatsleutel + 1
    df = df.withColumn("surrogaat_sleutel", row_number().over(window_spec) + (max_bestaande_surrogaatsleutel or 0))
    return df

def controleer_unieke_waarden(df: DataFrame, rij_id_var: str):
    """
    Controleert of alle waarden in een specifieke kolom uniek zijn in het gegeven DataFrame.

    Parameters:
    - df: DataFrame: Het DataFrame waarin de controle wordt uitgevoerd.
    - rij_id_var: str: De naam van de kolom waarvan de unieke waarden worden gecontroleerd.

    Returns:
    - None

    Als het aantal unieke waarden in de kolom niet gelijk is aan het totale aantal rijen,
    wordt er een melding geprint dat niet alle waarden in de kolom uniek zijn.
    
    Laatste update: 23-11-2023
    """
    # Tel het aantal unieke waarden in de kolom
    aantal_unieke = df.select(col(rij_id_var)).distinct().count()
    
    # Krijg het totale aantal rijen in het DataFrame
    totaal_aantal = df.count()
    
    # Controleer of het aantal unieke waarden gelijk is aan het totale aantal rijen
    if aantal_unieke != totaal_aantal:
        foutmelding = f"Niet alle waarden in de kolom '{rij_id_var}' zijn uniek."
        raise ValueError(foutmelding)

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

def initialiseer_historisering(df: DataFrame, schema_catalog: str, rij_id_var: str, naam_nieuw_df: str):
    """
    Bereid gegevens voor op historisering door historische data te markeren met relevante metadata.

    Parameters:
    - df (str): De naam van de temporary view die het huidige DataFrame vertegenwoordigt.
    - schema_catalog (str): Naam van het schema waar de tabel instaat of opgeslagen moet worden
    - rij_id_var (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
    - naam_nieuw_df (str): Naam van dataframe in de catalogus

    Laatste update: 04-12-2023
    """
    # Kijk of de opgegeven identifier uniek is
    controleer_unieke_waarden(df = df, rij_id_var = rij_id_var)

    # Controleer of het nieuwe DataFrame de vereiste kolommen bevat: begindatum_record, einddatum_record, record_actief, surrogaat_sleutel
    df.show()
    time.sleep(5)
    antwoord = input("Heb je de tabel bekeken en wil je deze voor de eerste keer opslaan met de vereiste kolommen? Type dan 'ja'")

    if antwoord.lower() == "ja":
        vereiste_kolommen = ['geldig_van', 'geldig_tot', 'surrogaat_sleutel', 'record_actief', "actie"]  #, "laad_datum", "gewijzigd_op"]
        ontbrekende_kolommen = [kolom for kolom in vereiste_kolommen if kolom not in df.columns]
    
        if ontbrekende_kolommen:
            foutmelding = f"Ontbrekende kolommen: {', '.join(ontbrekende_kolommen)}. Deze zullen aan het DataFrame worden toegevoegd..."
            print(foutmelding)
            
            # Roep de functie tijdzone_amsterdam aan om de correcte tijdsindeling te krijgen
            huidige_datum_tz = tijdzone_amsterdam()

            # Werk de einddatum, record_actief en begindatum kolommen bij in het huidige DataFrame
            temp_df = (df
                        .withColumn("geldig_van", huidige_datum_tz)
                        .withColumn("geldig_tot", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss"))
                        .withColumn("record_actief",     lit(True))
                        .withColumn("actie",     lit("insert"))
                      )

            # Voeg nieuwe surrogaatsleutels toe op basis van de maximale sleutel in de DWH
            output = voeg_aangepaste_surrogaatsleutel_toe(temp_df)
            output.display()

            # Sla de gegevens op in delta-formaat in het opgegeven schema
            print(f"De tabel wordt nu opgeslagen in {schema_catalog} onder de naam: {naam_nieuw_df}")
            output.write.saveAsTable(f'{schema_catalog}.{naam_nieuw_df}', mode='overwrite')

def bepaal_veranderde_records(huidig_df: DataFrame, nieuw_df: DataFrame, rij_id_var: str):
    """
    Identificeert en retourneert unieke rij-identificatoren van gewijzigde records tussen twee DataFrames op basis van een rij-id en verschillende kolommen.

    Parameters:
        huidig_df (DataFrame): Het huidige DataFrame met de bestaande gegevens.
        nieuw_df (DataFrame): Het nieuwe DataFrame met bijgewerkte gegevens.
        rij_id_var (str): De naam van de kolom die als rij-id wordt gebruikt om records te identificeren.

    Returns:
        set: Een verzameling met unieke rij-identificatoren voor de gewijzigde records tussen de huidige en nieuwe DataFrames.

    Voorbeeld:
        huidig_df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["account_id", "name"])
        nieuw_df = spark.createDataFrame([(2, "Bobby"), (3, "Charlie")], ["account_id", "name"])
        unieke_rij_id_veranderd = bepaal_veranderde_records(huidig_df, nieuw_df, "account_id")
        print(unieke_rij_id_veranderd)  # Output: {2, 3}
    Laatste update: 23-11-2023
    """
    
    # Pas huidig DataFrame aan voor de vergelijking
    uitzondering_kolom = ["geldig_van", "geldig_tot", "surrogaat_sleutel", "record_actief", "actie"]
    huidig_df_temp = huidig_df.drop(*uitzondering_kolom)

    # Controleer op basis van de nieuwe gegevens welke rijen in de huidige dataset zijn gewijzigd
    veranderde_df = nieuw_df.subtract(huidig_df_temp)
    
    # Haal de unieke rij-identificatoren op
    unieke_rij_id = veranderde_df.select(rij_id_var).distinct()
    unieke_rij_id_lijst = [rij[0] for rij in unieke_rij_id.collect()]
    unieke_rij_id_lijst = set(unieke_rij_id_lijst)

    return unieke_rij_id_lijst

def updaten_historisering_dwh(nieuw_df: DataFrame, rij_id_var: str, schema_catalog: str, naam_nieuw_df: str, huidig_dwh: str = None):
    """
    Voegt historische gegevens toe aan een PySpark DataFrame om wijzigingen bij te houden.

    Args:
        nieuw_df (DataFrame): DataFrame met gewijzigde gegevens.
        rij_id_var (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
        naam_nieuw_df (str): Naam van de tabel in het opgegeven schema.
        huidig_dwh (str, optioneel): Naam van het huidige DWH DataFrame. Indien niet opgegeven, wordt 
                                     de huidige tabelnaam van 'nieuw_df' gebruikt (komt overeen met het DWH).
    Laatste update: 4-12-2023
    """     
    # Lees de huidige versie van de opgegeven tabel in
    if huidig_dwh is None:
        huidig_dwh_tabel = spark.read.table(f"{schema_catalog}.{naam_nieuw_df}").cache()
    else:
        huidig_dwh_tabel = spark.read.table(f"{schema_catalog}.{huidig_dwh}").cache()

    # Roep de functie tijdzone_amsterdam aan om de correcte tijdsindeling te krijgen
    huidige_datum_tz = tijdzone_amsterdam() 

    # Bepaal de juiste volgorde van de kolommen
    dwh_kolom_volgorde = huidig_dwh_tabel.columns

    # Roep de functie bepaal_veranderde_records aan om de records te krijgen van rijen die veranderd zijn
    unieke_lijst_id = bepaal_veranderde_records(huidig_dwh_tabel, nieuw_df, rij_id_var)

    # Converteer de set van unieke id's naar een dataframe die je kunt broadcasten over verschillende workers
    id_df = spark.createDataFrame([(id,) for id in unieke_lijst_id], ["account_id"])

    # Dataframes opsplitsen in delen die wel aangepast moeten worden en ander deel niet.
    # Dit doen wij voor de efficiëntie en optimalisatie van de functie
    # Aangezien we met de id's filteren kan het contra-intuïtief zijn om bij geen_aanpassing een left_anti join te gebruiken
    huidig_dwh_tabel_geen_aanpassing = huidig_dwh_tabel.join(broadcast(id_df), "account_id", "left_anti")
    huidig_dwh_tabel_wel_aanpassing = huidig_dwh_tabel.join(broadcast(id_df), "account_id") 
    
    # Pas de records in het DWH aan die veranderd zijn
    temp_huidig_dwh_tabel = (huidig_dwh_tabel_wel_aanpassing
                             .withColumn("nieuwe_geldig_tot", huidige_datum_tz)
                             .withColumn("record_actief_update", lit(False))
                             .drop("geldig_tot").withColumnRenamed("nieuwe_geldig_tot", "geldig_tot")
                             .drop("record_actief").withColumnRenamed("record_actief_update", "record_actief")
                             .select(*dwh_kolom_volgorde)
                             )

    # Voeg de 2 delen van het DWH weer samen
    rejoined_huidig_dwh_tabel = huidig_dwh_tabel_geen_aanpassing.union(temp_huidig_dwh_tabel)

    # Dataframes filteren op de id's die aangepast zijn en met de nieuwe gegevens weer toegevoegd worden aan de DWH
    temp_nieuw_df_wel_aanpassing = nieuw_df.join(broadcast(id_df), "account_id") 
    temp_verandert_df = (temp_nieuw_df_wel_aanpassing
                         .withColumn("surrogaat_sleutel", lit(None))
                         .withColumn("record_actief", lit(True))
                         .withColumn("geldig_van", huidige_datum_tz)
                         .withColumn("geldig_tot", to_timestamp(lit("9999-12-31 23:59:59"), "yyyy-MM-dd HH:mm:ss"))
                         .withColumn("actie", lit("insert"))
                         )

    # Voeg nieuwe surrogaatsleutels toe
    max_surrogaat_sleutel = rejoined_huidig_dwh_tabel.select(max(rejoined_huidig_dwh_tabel["surrogaat_sleutel"])).first()[0]
    temp_verandert_df = voeg_aangepaste_surrogaatsleutel_toe(temp_verandert_df, max_surrogaat_sleutel)

    # Sorteer beide dataframe op kolommen, zodat ze samengevoegd kunnen worden
    gesorteerde_kolommen = sorted(rejoined_huidig_dwh_tabel.columns, key=lambda kol: kol.lower())
    df1 = rejoined_huidig_dwh_tabel.select(*gesorteerde_kolommen)
    df2 = temp_verandert_df.select(*gesorteerde_kolommen)
    output = df1.union(df2)
    output = output.select(*dwh_kolom_volgorde)

    # Sla de gegevens op in delta-formaat in de zilveren laag
    print(f"De tabel wordt nu opgeslagen in {schema_catalog} onder de naam: {naam_nieuw_df}")
    output.write.saveAsTable(f'{schema_catalog}.{naam_nieuw_df}', mode='overwrite')
    return

def toepassen_historisering(nieuw_df, schema_catalog: str, rij_id_var: str, naam_nieuw_df=None, huidig_dwh: str = None):
    """
    Deze regisseurfunctie roept op basis van bepaalde criteria andere functies aan en heeft hiermee de controle over de uitvoering van het historiseringsproces.

    Deze functie gaat ervan uit dat je een string opgeeft die verwijst naar een SQL temporary view of Python DataFrame. Wanneer jij bij nieuw_df een Python DataFrame opgeeft, moet je verplicht naam_nieuw_df invullen. Aangezien Python geen objectnaam kan afleiden van objecten.
    Args:
        nieuw_df (str of object): Naam van het nieuwe DataFrame dat verwijst naar een temporary view met gewijzigde gegeven of een Python DataFrame
        schema_catalog (str): Naam van het schema waar de tabel instaat of opgeslagen moet worden.
        rij_id_var (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
        naam_nieuw_df (str, verplicht bij opgegeven Python DataFrames): Naam van DataFrame/Tabel zoals die opgeslagen is in het opgegeven schema/catalog
        huidig_dwh (str, optioneel): Naam van het huidige DWH DataFrame. Indien niet opgegeven, wordt
                                     de huidige tabelnaam van 'nieuw_df' gebruikt (komt overeen met het DWH).

    Laatste update: 04-12-2023

    Raises:
        ValueError: Als de tabel/dataframe-naam niet kan worden afgeleid vanuit het object. 
                    Indien je bij nieuw_df een Python DataFrame meegeeft, moet je de naam van de tabel geven 
                    zoals bij naam_nieuw_df.
    """
    # Bepaal of de input een SQL temporary view is of een PySpark dataframe.
    if type(nieuw_df) == str:
        naam_nieuw_df = nieuw_df.replace("_temp_view", "")
        temp_nieuw_df = spark.table(nieuw_df).cache()
    elif type(nieuw_df) != str and (naam_nieuw_df is not None and naam_nieuw_df != ""):
        temp_nieuw_df = nieuw_df.cache()
    else:
         raise ValueError("Python kan de tabel/dataframe-naam niet afleiden vanuit het object. Indien je bij nieuw_df een Python DataFrame meegeeft, moet je de naam van de tabel geven zoals bij naam_nieuw_df")

    # Haal metadata op uit de Unity Catalog
    tabellen_catalog = spark.sql(f"SHOW TABLES IN {schema_catalog}")

    # Maak een set van alle tabellen in het opgegeven schema
    set_tabellen_catalog = {row["tableName"] for row in tabellen_catalog.collect()}

    # Controleer of de opgegeven tabel al bestaat in het opgegeven schema
    if naam_nieuw_df in set_tabellen_catalog:
        print(f"De tabel: {naam_nieuw_df} bevindt zich in de Unity Catalogus onder het volgende schema: {schema_catalog}")
        updaten_historisering_dwh(nieuw_df=temp_nieuw_df, schema_catalog=schema_catalog, rij_id_var=rij_id_var, naam_nieuw_df=naam_nieuw_df)
    else:
        print(f"Dit is de eerste keer dat je de tabel: {naam_nieuw_df} wilt historiseren. Historisering wordt nu toegepast...")
        initialiseer_historisering(df=temp_nieuw_df, schema_catalog=schema_catalog, rij_id_var=rij_id_var, naam_nieuw_df=naam_nieuw_df)
    return "Historisering is toegepast!"
