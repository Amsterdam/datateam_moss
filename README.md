# 1. Introductie
In deze package staan functies die door het datateam MOSS+ van de gemeente Amsterdam gebruikt worden in de data pipelines. Aangezien dit datateam 4 directies bedient, zijn er dezelfde functies die voor verschillende projecten gebruikt worden. Om te zorgen dat er geen wildgroei ontstaat van losse functies of verschillende versies van dezelfde functies is er gekozen om een package te maken die geïmporteerd kan worden.

## 1.1 Disclaimers
>> **Deze repo zit in de Proof of Concept fase**

# 2. Vereiste
Binnen het cluster Digitalisering, Innovatie en Informatie (DII) zit verschillende directies; waaronder de directie Data. Binnen de directie data is er in besloten om het datalandschap te moderniseren. Hiervoor is er gekozen om over te stappen naar Databricks. De functies die jij aanroept via deze packages zijn gemaakt/getest op de Azure Databricks omgeving. Andere omgevingen of andere, niet hieronder genoemde, clusterconfiguraties worden (nog) niet gesupport. 

## 2.1 Geteste Databricks Cluster Configuraties

| Cluster Configuratie nr |Access mode | Node | Databricks Runtime version | Worker type | Min Workers | Max Workers | Driver type |
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| cluster_configuratie_1 | Single User | Multi | Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1) | Standard_DS3_v2 (14gb Memory, 4 Cores) | 2 | 8 | Standard_DS3_v2 (14GB Memory, 4 Cores) |
| cluster_configuratie_2 | Single User | Single | Runtime: 14.2 (includes Apache Spark 3.5.0, Scala 2.12) | Standard_DS3_v2 (14gb Memory, 4 Cores) | 1 | 1 | Standard_DS3_v2 (14GB Memory, 4 Cores)|


# 3. Overzicht Functionaliteiten
- 3.1 -> Algemene functie
- 3.2 -> Reversed modeling voor logische modellen
 
## 3.1 Algemene functies
Op dit moment zijn er 2 functies beschikbaar:

```python
clean_colum_names(df.columns) # deze functies schoont kolomnamen op
rename_multiple_columns(df, {'oude_naam': 'nieuwe_naam', 'oude_naam_2': 'nieuwe_naam_2'}) # deze hernoemt meerdere kolommen
```      

## 3.2 Reversed modeling voor logische modellen

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
