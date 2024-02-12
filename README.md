# 1. Introductie
In deze package staan functies die door het datateam MOSS+ van de gemeente Amsterdam gebruikt worden in de data pipelines. Aangezien dit datateam 4 directies bedient, zijn er dezelfde functies die voor verschillende projecten gebruikt worden. Om te zorgen dat er geen wildgroei ontstaat van losse functies of verschillende versies van dezelfde functies is er gekozen om een package te maken die geïmporteerd kan worden.

# 2. Vereiste
Binnen het cluster Digitalisering, Innovatie en Informatie (DII) zit verschillende directies; waaronder de directie Data. Binnen de directie data is er in besloten om het datalandschap te moderniseren. Hiervoor is er gekozen om over te stappen naar Databricks. De functies die jij aanroept via deze packages zijn gemaakt/getest op de Azure Databricks omgeving. Andere omgevingen of andere, niet hieronder genoemde, clusterconfiguraties worden (nog) niet gesupport. 

## 2.1 Geteste Databricks Cluster Configuraties

| Cluster Configuratie nr |Access mode | Node | Databricks Runtime version | Worker type | Min Workers | Max Workers | Driver type |
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| cluster_configuratie_1 | Single User | Multi | Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1) | Standard_DS3_v2 (14gb Memory, 4 Cores) | 2 | 8 | Standard_DS3_v2 (14GB Memory, 4 Cores) |
| cluster_configuratie_2 | Single User | Single | Runtime: 14.2 (includes Apache Spark 3.5.0, Scala 2.12) | Standard_DS3_v2 (14gb Memory, 4 Cores) | 1 | 1 | Standard_DS3_v2 (14GB Memory, 4 Cores)|

## 2.2 Performance
De code voor het historiseren (Slowly Changing Dimensions Type 2) is (zover mogelijk) volledig geoptimaliseerd voor PySpark op databricks. Echter kan het zo zijn dat bij grote datasets de functie minder snel werkt. De oplossing, zoals je in de tabel hieronder kunt zien, is het gebruiken van meer computerkracht, workers en multinodes. Het is een afweging die je zelf moet maken: is tijd of zijn kosten belangrijker? 

| Cluster Configuratie nr | n_records | deel | tijd | 
| ------ | ------ | ------ | ------ |
| cluster_configuratie_1 | 7.000.000 | initialiseren | 1.27 min |
| cluster_configuratie_2 | 7.000.000 | initialiseren | 1.35 min |
| cluster_configuratie_2 | 5.000.000 | initialiseren | 1.41 min |
| cluster_configuratie_2 | 2.000.000 | initialiseren | 0.5 min (32 sec) |
| cluster_configuratie_1 | 7.000.000 | updaten | 3.43 min |
| cluster_configuratie_2 | 7.000.000 | updaten | 10.98 min |
| cluster_configuratie_2 | 5.000.000 | updaten | 8.01 min |
| cluster_configuratie_2 | 2.000.000 | updaten | 5.06 min |


## 2.3 Bottlenecks
> De code is geanalyseerd op onderdelen die veel tijd kosten. Mocht je deze code willen verbeteren, stuur dan een pull request. 

| Verbeterpunten | Functie | Onderdeel code |
| ---- | ---- | ---- |
| Het opslaan van de losse partities kost op dit moment, relatief te meeste tijd | updaten_historisering_dwh | output.write.saveAsTable() | 

## 2.4 Disclaimers
>> **Deze repo zit in de Proof of Concept fase**

# 3. Overzicht Functionaliteiten
- 3.1 -> Historisering
- 3.2 -> Algemene functie
- 3.3 -> Reversed modeling voor logische modellen
  
## 3.1 Historisering
In deze repo vind je functies voor het historiseren van tabellen. In het specifiek het toepassen van slowly changing dimensions type 2. 
Voor het gebruik van de historisering functies volg het volgende stappenplan:

```python
#### Let op! ####
# Het beste is om de package de installeren op jouw cluster.
# De MOSS Package staat op PyPi dus wanneer je dit op jouw cluster wilt installeren,
# kies het PyPi-menu voor het installeren van de package.
# Elke keer als het cluster opnieuw opstart wordt de recenste versie ingeladen.

# Wil je het handmatig in jouw databricks sessie installeren gebruik dan de code hieronder. Voer dit uit in een databricks cel 
!pip install datateam-moss
#### Let op! ####

# Wanneer jij de package geïnstalleerd hebt, moet je de package nog inladen.
import datateam_moss as dpms

# Nu kan je de verschillende functies aanroepen. De regisseursfunctie voor historisering is toepassen_historisering().
# Dit doe je, bijvoorbeeld, als volgt:
dpms.toepassen_historisering(bestaande_tabel=df_cleaned, schema_catalog=f"{CATALOG}.{layer}",
                             naam_tabel="crm_locatieorganisaties", business_key="ams_locatieorganisatieid",
                             naam_bk="mtd_locatieorganisatie_bk",  naam_id="mtd_locatieorganisatie_id")

# Hieronder nog de documentatie van regisseursfunctie. Je zou dit zelf ook kunnen opzoeken in de /src-map.
 """
    Deze regisseurfunctie roept op basis van bepaalde criteria andere functies aan en
      heeft hiermee de controle over de uitvoering van het historiseringsproces.

    Deze functie gaat ervan uit dat je een string opgeeft die verwijst naar
        een SQL temporary view of Python DataFrame.
    Wanneer jij bij bestaande_tabel een Python DataFrame opgeeft, moet je verplicht naam_tabel invullen.
    Aangezien Python geen objectnaam kan afleiden van objecten.
    
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
        overwrite_schema (bool): Met deze boolean kun je aangeven of het schema van de tabel overschreven mag worden.
                                 - True -> mag overschreven worden
                                 - False -> mag NIET overschreven worden (default) 
    """   
```


De functie heeft de volgende functionaliteit:
| kolomnaam | beschrijving | voorbeeld | 
| ------ | ------ | ------ |
| {naam_id} | Dit is een integer waarin o.b.v. een unieke business identifier bepaalt wordt of er dubbele records inzitten. Het kan namelijk zo zijn dat er bepaalde kolommen veranderen over tijd. De surrogaatsleutel zorgt ervoor dat dezelfde business identifier uniek is. | 1 | 
| mtd_record_actief | Deze kolom bepaalt of de record actief is. Het kan namelijk voorkomen dat o.b.v. een identifier dubbele records zijn, waarvan 1 record historisch is. Door deze kolom kan je makkelijker filteren op de actieve records | True |
| mtd_geldig_van | Deze kolom geeft aan vanaf welk moment de record actief is. De tijd wordt aangegeven met de Nederlandse tijdzone en nauwkeurig tot op de seconde | 2023-12-31 23:59:59 | 
| mtd_geldig_tot | Deze kolom geef aan op welk moment de record aangepast is. Als er een aanpassing heeft plaatsgevonden, is dit niet meer het de recenste record. De record wordt gesloten op de aanpassingdatum en er wordt een nieuw record, met de verandering, aangemaakt. | 9999-12-31 23:59:59 |
| mtd_actie | Deze kolom geeft aan wat er met de record gebeurd is: inserted, changed, deleted, reinserted | inserted |

Verder controleert deze functies of de opgegeven business_key uniek is in de aangeleverde tabel. Als er dubbele business keys inzitten kan er niet bepaald worden wat er met de record moet gebeuren.

## 3.2 Algemene functies
Op dit moment zijn er 2 functies beschikbaar:

```python
clean_colum_names(df.columns) # deze functies schoont kolomnamen op
rename_multiple_columns(df, {'oude_naam': 'nieuwe_naam', 'oude_naam_2': 'nieuwe_naam_2'}) # deze hernoemt meerdere kolommen
```      

## 3.3 Reversed modeling voor logische modellen

##### Gebruik
```python
dataset = Dataset(
    catalog='catalog_name', # bijv. 'dpms_dev'
    database='schema_name', # bijv. 'silver'
    project_prefixes=['prefix_'], # bijv. 'sport_'
    exclude=['test'] 
)

dataset.print_eraser_code()
dataset.print_drawio_code()
```


##### Samenvatting
Deze code omvat reversed modeling; het geeft de input voor tools voor visuele logische modellen op basis van bestaande tabellen en kolomnamen in Databricks.

##### 1. Het doel van deze code
Deze code geeft in tekstvorm de input van een visueel logisch model in Eraser.io of DrawIO. Dit maakt reversed modeling mogelijk: eerst worden tabellen in Databricks gemaakt, daarna leidt deze code het model daaruit af. Dit maakt het modelleerproces efficienter.

##### 2. Hoe de code werkt en onderliggende aannames
De code kijkt binnen een catalog (bv. 'dpms_dev') en binnen een database (bv 'silver') en dan weer binnen een door de eindgebruiker gedefinieerde naam voor een project (bv 'amis' in de tabelnaam) naar een set tabellen en kolomnamen. Dit resulteert in de volgende metadata: 
1. tabellen met kolommen met namen en datatypes: deze corresponderen exact met Databricks
2. de relaties tussen die tabellen. Hier zijn enkele aannames gedaan:
    - Er is sprake van een relatie tussen 2 tabellen, als ze beide een kolom met dezelfde naam hebben
    - De kardinaliteit (bv. one-to-many) wordt afgeleid van de unieke waarden in deze kolommen. Bijvoorbeeld: voor account_id in tabel 1 is de COUNT gelijk aan de COUNT DISTINCT: per rij is er een unieke account_id, dus dit is een one-to??? relatie. In tabel 2 is de COUNT van account_id hoger dan de COUNT DISTINCT: er is geen unieke id per rij. Dit maakt een one-to-many relatie.

##### 3. Ondersteuning voor tools
Deze tools worden momenteel ondersteund (dit kan in de toekomst nog uitbreiden):
- Eraser.IO (ERD Schema from code)
- DrawIO (CREATE TABLE statement)

##### 4. Toekomstige features
Deze features staan op de backlog:
- Een lijst van project prefixes ingeven
- Project prefixen leeg laten, zodat de hele database (bv. zilveren laag) wordt uitgelezen
"""
