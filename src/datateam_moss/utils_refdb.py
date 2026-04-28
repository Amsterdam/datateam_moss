def drop_objecten_in_schema(catalog: str, schema: str):
    """
    Verwijdert alle tabellen en views binnen een opgegeven schema in een catalog.

    Deze functie haalt alle objecten (tabellen en views) op via de 
    information_schema en verwijdert deze één voor één. Views worden verwijderd 
    met `DROP VIEW` en tabellen met `DROP TABLE`.

    Parameters
    ----------
    catalog : str
        De naam van de catalog waarin het schema zich bevindt.
    schema : str
        De naam van het schema waarvan alle objecten verwijderd moeten worden.
    """

    objects = spark.sql(f"""
        SELECT table_name, table_type
        FROM `{catalog}`.information_schema.tables
        WHERE table_schema = '{schema}'
    """).collect()

    for obj in objects:
        name = obj.table_name
        if obj.table_type == 'VIEW':
            spark.sql(f"DROP VIEW `{catalog}`.`{schema}`.`{name}`")
        else:
            spark.sql(f"DROP TABLE `{catalog}`.`{schema}`.`{name}`")

def schrijf_tabel_naar_refdb(pad_unity_catalog: str, pad_refdb: str,driver: str,user: str,password: str,url: str):
    '''
    Schrijft een individuele tabel weg naar de Referentiedatabase
    args:
    pad_unity_catalog: str
        pad naar tabel in Unity Catalog
    pad_refdb: str
        pad naar tabel in RefDB
    '''
    try:
        df = spark.table(pad_unity_catalog)
        (df 
            .write 
            .format("jdbc") 
            .option("driver", driver) 
            .option("url", url) 
            .option("user", user) 
            .option("password", password) 
            .option("ssl", True) 
            .option("sslmode", "require") 
            .option("truncate", "true") 
            .option("dbtable", pad_refdb) 
            .mode("overwrite") 
            .save())

    except Exception as e:
        raise RuntimeError(
            f"Pipeline gestopt bij {pad_unity_catalog} → {pad_refdb}: {e}"
        )

def schrijf_dataset_naar_refdb(catalog: str, schema_unity_catalog: str, driver: str, user: str, password: str, url: str, tabellen: List=None) -> None:
    '''
    Schrijft alle tabellen in een schema naar de Referentiedatabase, of neemt een selectie indien een list is opgegeven. Het wegschrijven is beperkt tot het schema 'public' in de Referentiedatabase

    args:
    catalog: str
        catalog van de Unity Catalog
    schema_unity_catalog: str
        schema van de Unity Catalog
    tabellen: list
        optioneel: lijst van tabellen die naar de RefDB moeten worden geschreven
    '''
    if tabellen is None:
        tabel_namen = [t.name for t in spark.catalog.listTables(schema_unity_catalog)]
    else:
        tabel_namen = tabellen

    for naam in tabel_namen:
        pad_unity_catalog = f"{catalog}.{schema_unity_catalog}.{naam}"
        pad_refdb = f"public.{naam}"
        schrijf_tabel_naar_refdb(pad_unity_catalog,
                                 pad_refdb,
                                 driver=driver,
                                 user=user,
                                 password=password,
                                 url=url)

        if catalog.endswith('dev'):
            print(f"Tabel {pad_unity_catalog} naar {pad_refdb} weggeschreven")
