# Databricks notebook source
from itertools import combinations
from databricks.sdk.runtime import *
import pandas as pd

class Dataset:
    """
    Represents a dataset in a Databricks catalog.database.

    Usage:
        dataset = Dataset(
            catalog='catalog_name', # bijv. 'dpms_dev'
            database='schema_name', # bijv. 'silver'
            project_prefixes=['prefix_'], # bijv. 'sport_'
            exclude=['test'] 
        )
        dataset.print_eraser_code()
        dataset.print_drawio_code()

    Args:
        spark_session (SparkSession): The SparkSession to use, which is already loaded into each databricks notebooks as 'spark'.
        catalog (str): The name of the catalog.
        database (str): The name of the database.
        project_prefix (list, optional): A list of project prefixes to filter tables. Defaults to [].
        exclude (list, optional): A list of table names to exclude. Defaults to [].
    """

    def __init__(self, spark_session, catalog: str, database: str, project_prefixes: list = [], exclude: list = []):
        self.spark = spark_session
        self.catalog = catalog
        self.database = database
        self.spark.sql(f"USE catalog {catalog}") # dit werkt in persoonlijke en shared clusters 
        self.spark.sql(f"USE schema {database}") # dit werkt in persoonlijke en shared clusters
        self.project_prefixes = project_prefixes
        self.exclude = exclude
        self.tables = self.get_tables()
        self.tables_dict = self.get_tables_dict()
        self.occurrences = self.get_column_occurrences()
        self.relations = self.get_relations()
        self.type_of_relations = {}

    def get_tables(self):
        """
        Retrieves the list of tables in the dataset.

        Returns:
            list: A list of tables in the dataset.
        """
        table_df = (spark.sql(f"SHOW TABLES").select("tableName").collect())
        tables = [row["tableName"] for row in table_df]
        if self.project_prefixes == [] or not self.project_prefixes:
            return tables
        else:
            return [t for t in tables if any(s in t for s in self.project_prefixes) and not t in self.exclude]
    
    def get_tables_dict(self):
        """
        Retrieves a dictionary of table names and their corresponding schemas.

        Returns:
            dict: A dictionary of table names and their corresponding schemas.
        """
        return {t: self.spark.table(t).schema for t in self.tables}

    def get_column_occurrences(self):
        """
        Retrieves a dictionary of column names and the tables where they occur.

        Returns:
            dict: A dictionary of column names and the tables where they occur.
        """
        occurrences = {}
        for table in self.tables:
            for c in self.spark.table(table).schema:
                occurrences.setdefault(c.name, []).append(table)
        return occurrences
    
    def get_relations(self):
        """
        Retrieves a list of relations between columns in different tables.

        Returns:
            list: A list of relations between columns in different tables.
        """
        relations = []
        for c, tables in self.occurrences.items():
            if len(tables) > 1:
                for table1, table2 in combinations(tables, 2):
                    df1 = self.spark.table(table1).select(c)
                    df2 = self.spark.table(table2).select(c)

                    if df1.distinct().count() == df1.count() and df2.distinct().count() == df2.count():
                        relation_type = '-'
                    elif df1.distinct().count() != df1.count() and df2.distinct().count() == df2.count():
                        relation_type = '<'
                    elif df1.distinct().count() == df1.count() and df2.distinct().count() != df2.count():
                        relation_type = '>'
                    else:
                        relation_type = '<>'

                    relations.append((f'{table1}.{c}', f'{table2}.{c}', relation_type))
        return relations

    def print_eraser_code(self):
        """
        Prints out the full code for creating an ER Diagram in eraser.io for the dataset.
        """
        for table_name, schema in self.tables_dict.items():
            print(table_name, '{')
            for c in schema:
                print('\t', c.name, c.dataType.simpleString())
            print('}')
            print()

        # print relations
        for c1, c2, r_type in self.relations:
            print(f' {r_type} '.join((c1, c2)))

    def print_drawio_code(self):
        """
        Prints out the code for creating a Draw.io diagram for the dataset.
        """
        for table_name, schema in self.tables_dict.items():
            print('CREATE TABLE', table_name)
            for c in schema:
                print(c.name)
            print()


    def get_table_stats(self, table_name):
        """
        Retrieves the table statistics for a given table.

        Args:
            table_name (str): The name of the table.

        Returns:
            pandas.DataFrame: The table statistics.
        """
        # return self.spark.table(table_name).toPandas().describe()
        df = self.spark.table(table_name).toPandas()
        return pd.concat([
            df.nunique().rename('count_distinct'),
            (df.nunique() / len(df)).rename('pct_distinct'),
            df.isnull().mean().rename('pct_null')
        ], axis=1)


    def print_stats(self, table_name=None):
        """
        Prints out the table statistics for all tables in the dataset or one provided.

        Args:
            table_name (str, optional): The name of the table. Defaults to None.

        Only prints the table statistics in markdown.
        """
        if table_name == None:
            for table_name in self.tables:
                print(table_name)
                print(self.get_table_stats(table_name).to_markdown())
                print()
        else:
            print(table_name)
            print(self.get_table_stats(table_name).to_markdown())
            print()
