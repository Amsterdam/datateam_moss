# 1. Introductie
In deze package staan functies die door het datateam MOSS+ van de gemeente Amsterdam gebruikt worden in de data pipelines. Aangezien dit datateam 4 directies bedient, zijn er dezelfde functies die voor verschillende projecten gebruikt worden. Om te zorgen dat er geen wildgroei ontstaat van losse functies of verschillende versies van dezelfde functies is er gekozen om een package te maken die geïmporteerd kan worden.

# 2. Vereiste
Binnen het cluster Digitalisering, Innovatie en Informatie (DII) zit verschillende directies; waaronder de directie Data. Binnen de directie data is er in besloten om het datalandschap te moderniseren. Hiervoor is er gekozen om over te stappen naar Databricks. De functies die jij aanroept via deze packages zijn gemaakt/getest op de Azure Databricks omgeving. Andere omgevingen of andere, niet hieronder genoemde, clusterconfiguraties worden niet gesupport. 

## 2.1 Geteste Databricks Cluster Configuraties

|Access mode | Node | Databricks Runtime version | Worker type | Min Workers | Max Workers | Driver type |
| ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| Singe User | Multi node | Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1) | Standard_DS3_v2 (14gb Memory, 4 Cores) | 2 | 8 | Standard_DS3_v2 (14GB Memory, 4 Cores) |

## 2.2 Disclaimers
>> **Deze repo zit in de Proof of Concept fase** 

# 3. Overzicht

## 3.1 Historisering
In deze repo vind je functies voor het historiseren van tabellen. In het specifiek het toepassen van slowly changing dimensions type 2. 
Voor het gebruik van de historisering functies volg het volgende stappenplan:

```python
# Voer dit uit in een databricks cel
!pip install datateam-moss

# Wanneer jij de package geïnstalleerd hebt, moet je de package nog inladen.
import dpms

# Nu kan je de verschillende functies aanroepen. De regisseursfunctie voor historisering is toepassen_historisering(). Dit doe je als volgt:
dpms.toepassen_historisering(nieuw_df, schema_catalog: str, rij_id_var: str, naam_nieuw_df=None, huidig_dwh: str = None):


# Hieronder nog de documentatie van regisseursfunctie. Je zou dit zelf ook kunnen opzoeken in de /src-map.
def toepassen_historisering(nieuw_df, schema_catalog: str, rij_id_var: str, naam_nieuw_df=None, huidig_dwh: str = None):
    """
    Deze regisseurfunctie roept op basis van bepaalde criteria andere functies aan en heeft hiermee de controle over de uitvoering van het historiseringsproces.

    Deze functie gaat ervan uit dat je een string opgeeft die verwijst naar een SQL temporary view of Python DataFrame.
    Wanneer jij bij nieuw_df een Python DataFrame opgeeft, moet je verplicht naam_nieuw_df invullen. Aangezien Python geen objectnaam kan afleiden van objecten.
    Args:
        nieuw_df (str of object): Naam van het nieuwe DataFrame dat verwijst naar een temporary view met gewijzigde gegeven of een Python DataFrame
        schema_catalog (str): Naam van het schema waar de tabel instaat of opgeslagen moet worden.
        rij_id_var (str): Naam van de kolom die wordt gebruikt om unieke rijen te identificeren.
        naam_nieuw_df (str, verplicht bij opgegeven Python DataFrames): Naam van DataFrame/Tabel zoals die opgeslagen is in het opgegeven schema/catalog
        huidig_dwh (str, optioneel): Naam van het huidige DWH DataFrame. Indien niet opgegeven, wordt
                                     de huidige tabelnaam van 'nieuw_df' gebruikt (komt overeen met het DWH).
    Raises:
        ValueError: Als de tabel/dataframe-naam niet kan worden afgeleid vanuit het object. 
                    Indien je bij nieuw_df een Python DataFrame meegeeft, moet je de naam van de tabel geven 
                    zoals bij naam_nieuw_df.
    """
    return
```




De functie heeft de volgende functionaliteit:
| kolomnaam | beschrijving | voorbeeld | 
| ------ | ------ | ------ |
| surrogaat_sleutel | Dit is een integer waarin o.b.v. een unieke business identifier bepaalt wordt of er dubbele records inzitten. Het kan namelijk zo zijn dat er bepaalde kolommen veranderen over tijd. De surrogaatsleutel zorgt ervoor dat dezelfde business identifier uniek is. | 1 | 
| record_actief | Deze kolom bepaalt of de record actief is. Het kan namelijk voorkomen dat o.b.v. een identifier dubbele records zijn, waarvan 1 record historisch is. Door deze kolom kan je makkelijker filteren op de actieve records | True |
| geldig_van | Deze kolom geeft aan vanaf welk moment de record actief is. De tijd wordt aangegeven met de Nederlandse tijdzone en nauwkeurig tot op de seconde | 2023-12-31 23:59:59 | 
| geldig_tot | Deze kolom geef aan op welk moment de record aangepast is. Als er een aanpassing heeft plaatsgevonden, is dit niet meer het de recenste record. De record wordt gesloten op de aanpassingdatum en er wordt een nieuw record, met de verandering, aangemaakt. | 9999-12-31 23:59:59 |


## 3.2 Algemene functies
Op dit moment is er 1 functie beschikbaar. Deze functies schoont kolomnamen op:

```python
dpms.clean_columnames(cols)
```      
