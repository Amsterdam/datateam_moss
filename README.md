# 1. Introductie
In deze package staan functies die door het datateam MOSS+ van de gemeente Amsterdam gebruikt worden in de data pipelines. Aangezien dit datateam 4 directies bedient, zijn er dezelfde functies die voor verschillende projecten gebruikt worden. Om te zorgen dat er geen wildgroei ontstaat van losse functies of verschillende versies van dezelfde functies is er gekozen om een package te maken die geïmporteerd kan worden.

# 2. Vereiste
Binnen het cluster Digitalisering, Innovatie en Informatie (DII) zit verschillende directies; waaronder de directie Data. Binnen de directie data is er in besloten om het datalandschap te moderniseren. Hiervoor is er gekozen om over te stappen naar Databricks. De functies die jij aanroept via deze packages zijn gemaakt/getest op de Azure Databricks omgeving. Andere omgevingen of andere, niet hieronder genoemde, clusterconfiguraties worden niet gesupport. 

## 2.1 Geteste Databricks Cluster Configuraties

| Node | Databricks Runtime version | Worker type | Min Workers | Max Workers | Driver type |
| ------ | ------ | ------ | ------ | ------ | ------ |
| Multi node | Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1) | Standard_DS3_v2 (14gb Memory, 4 Cores) | 2 | 8 | Standard_DS3_v2 (14GB Memory, 4 Cores) |

# 3. Overzicht
## 3.2 


## 3.1 Historisering
In deze repo vind je functies voor het historiseren van tabellen. In het specifiek het toepassen van slowly changing dimensions type 2. 
Voor het gebruik van de historisering functies volg het volgende stappenplan:

```python
# Voer dit uit in een databricks cel
!pip install datateam-moss

# Wanneer jij de package geïnstalleerd hebt, moet je de package nog inladen.
import dpms

# Nu kan je de verschillende functies als volgt aanroepen:
# dpms.toepassen_historisering
# dpms.clean_columnames
```



## 3.2 Algemene functies


# 3. Disclaimers
- Deze repo is in de Proof of Concept fase
- De functies kunnen alleen gebruikt worden met een Personal Compute Cluster

        
