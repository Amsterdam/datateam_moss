import pyspark.sql.functions as F
from pyspark.sql.window import Window

def filter_pivot_bbga(df,variabelen,meest_recente_jaar = True):
    '''
    Filtert het Basisbestand Gebieden Amsterdam (BBGA) op variabelen en het meeste recente jaar of niet.
    args: 
    df: de BBGA in de vorm van een PySpark DataFrame
    variabelen: een lijst van variabelen of een dictionary met variabelen als keys en aliassen als values
    meest_recente_jaar: boolean, standaard True
    return: een PySpark DataFrame met de gewenste variabelen en het meest recente jaar tenzij anders aangegeve
    '''
    # Filter op variabelen en alias indien dictionary
    if isinstance(variabelen, dict):
        # Maak een mapping expression om kolomwaarden te hernoemen (row-based equivalent van kolommen aliasen)
        mapping_expr = F.create_map(*[F.lit(x) for x in sum(variabelen.items(), ())])

        # Filter het df met een where clause om alleen records met de gewenste indicator over te houden
        df_gefilterd = df.filter(F.col("Indicatordefinitieid").isin(list(variabelen.keys())))

        # Pas de mapping toe op de bestaande kolom
        df_gefilterd = df_gefilterd.withColumn("Indicatordefinitieid", mapping_expr[F.col("Indicatordefinitieid")])

    elif isinstance(variabelen, list):
        df_gefilterd = df.filter(F.col("Indicatordefinitieid").isin(variabelen))

    else:
        raise TypeError(
            "variabelen moet een list of dict zijn"
        )

    # Filter op het meest recente jaar 
    df_gefilterd_jaar = df_gefilterd.withColumn("Jaar", F.col("Jaar").cast("int"))
    df_gefilterd_jaar = df_gefilterd_jaar.withColumn("Waarde", F.col("Waarde").cast("double"))

    # Zet binnen window van gebied en indicator een row number op descending Jaar en neem rij 1 voor het meest recente jaar, groupby alleen op gebied  
    if meest_recente_jaar:
        window_spec = (
            Window
            .partitionBy("Gebiedcode15", "Indicatordefinitieid")
            .orderBy(F.col("Jaar").desc())
        )

        df_gefilterd_jaar = (
            df_gefilterd_jaar
            .withColumn("row_number", F.row_number().over(window_spec))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
        )

        # Pivot het gefilterde df
        df_pivot = (
        df_gefilterd_jaar
        .groupBy("Gebiedcode15")
        .pivot("Indicatordefinitieid")
        .agg(F.first("Waarde"))
        )

    # Indien meerdere jaren gewenst zijn, pivot en groupby op zowel gebied als jaar (anders wordt jaar gedropt en een random eerste waarde gepakt door First)
    else:
        # Pivot het gefilterde df
        df_pivot = (
        df_gefilterd_jaar
        .groupBy("Gebiedcode15","Jaar")
        .pivot("Indicatordefinitieid")
        .agg(F.first("Waarde"))
        )    
      
    return df_pivot
