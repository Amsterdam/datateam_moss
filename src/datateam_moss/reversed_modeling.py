# Databricks notebook source
import json
from itertools import combinations
from collections import defaultdict

import pandas as pd
from databricks.sdk.runtime import *
from pyspark.sql.functions import col


class Dataset:
    """
    Represents a dataset in a Databricks catalog.schema.

    Usage:
        dataset = Dataset(
            spark_session=spark, # already loaded into each databricks notebooks as 'spark'
            catalog='catalog_name', # e.g. 'dpms_dev'
            schema='schema_name', # e.g. 'gold'
            dataset='dataset_name', # e.g. 'amis'
            excluded_table_names=['amis_test1', 'amis_test2']
        )
        contract = dataset.create_datacontract()

    Args:
        spark_session (SparkSession): The SparkSession to use, which is already loaded into each databricks notebooks as 'spark'.
        catalog (str): The name of the catalog.
        schema (str): The name of the schema. Defaults to 'gold'.
        dataset (str): The name of the dataset used as prefix for the tables. Defaults to '', using all tables in the schema.
        excluded_table_names (list, optional): A list of table names to exclude. Defaults to [].
    """

    def __init__(self, spark_session, catalog: str, schema: str = 'gold', dataset: str = '', excluded_table_names: list = []):
        self.spark = spark_session
        self.catalog = catalog
        self.schema = schema
        self.dataset = dataset
        self.excluded_table_names = excluded_table_names
        self.information_schema = self.get_information_schema()
        self.tables_dict = self.get_tables_dict()

    def get_information_schema(self):
        """
        Retrieves all tables and columns in the schema with dataset_ as prefix to create a datacontract.

        Args:
            dataset (str): The name of the dataset.

        Returns:
            spark.DataFrame: A DataFrame with all columns and types in the tables.
        """
        # Retrieve tables, columns, and column tags from the information_schema
        tables = self.spark.table(f"{self.catalog}.information_schema.tables")
        columns = self.spark.table(f"{self.catalog}.information_schema.columns")
        column_tags = self.spark.table(f"{self.catalog}.information_schema.column_tags").filter("tag_name = 'term'")

        # Join tables, columns, and column tags to get the required information
        df = (tables
            .join(columns, 
                    (col("tables.table_name") == col("columns.table_name")) & 
                    (col("tables.table_schema") == col("columns.table_schema")), 
                    "inner")
            .join(column_tags, 
                    (col("column_tags.table_name") == col("columns.table_name")) & 
                    (col("column_tags.schema_name") == col("columns.table_schema")), 
                    "left_outer")
            .filter(
                (col("columns.table_schema") == self.schema) & 
                col("columns.table_name").like(f"%{self.dataset}_%") & 
                ~col("columns.table_name").isin(self.excluded_table_names)
            )

            # Select the required columns and order them
            .select(
                col("columns.table_name").alias("table_name"),
                col("columns.column_name").alias("column_name"),
                col("columns.full_data_type").alias("full_data_type"),
                col("columns.comment").alias("comment"),
                col("column_tags.tag_value").alias("tag_value")
            )
            .orderBy("columns.table_name", "columns.ordinal_position")
        )

        return df

    def _map_types_datacontract(self, type):
        """
        Maps the types of the columns to the types in the datacontract according to amsterdam-schema "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema".
        
        Args:
            type (str): The type of the column.

        Returns:
            str: The mapped type of the column.
        """
        return {
            'date': 'date',
            'string': 'string',
            'int': 'integer'
        }.get(type, 'string')

    def create_datacontract(
        self,
        version: str = 'v0',
        description: str = f'Description of the dataset',
        business_goal: str = 'Business goal of the dataset',
        theme: str = 'X',
        collection: str = 'X',
        datateam: str = 'MOSS',
        product_owner: str = 'x.name@amsterdam.nl',
        data_steward: str = '',
        language: str = 'Nederlands',
        confidentiality: str = 'Confidential',
        bio_quickscan: str = 'Done',
        privacy: str = 'Niet persoonlijk identificeerbaar',
        privacy_quickscan: str = 'Done',
        geo_data: str = '',
        history_start: str = '',
        refresh_rate: str = '',
        end_date: str = '',
        write_to_landingzone: bool = False,
        output_path: str = None
    ) -> str:
        """
        Creates a datacontract for the dataset for Data Management.

        TODO: add amsterdam-schema "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema" options.

        Args:
            version (str, optional): The version of the datacontract. Defaults to 'v0'.
            description (str, optional): The description of the dataset. Defaults to 'Description of the dataset'.
            business_goal (str, optional): The business goal of the dataset. Defaults to 'Business goal of the dataset'.
            theme (str, optional): The theme of the dataset. Defaults to 'X'.
            collection (str, optional): The collection of the dataset. Defaults to 'X'.
            datateam (str, optional): The name of the data team. Defaults to 'MOSS'.
            product_owner (str, optional): The email address of the data product owner. Defaults to 'x.name@amsterdam.nl'.
            data_steward (str, optional): The data steward of the dataset. Defaults to ''.
            language (str, optional): The language of the dataset. Defaults to 'Nederlands'.
            confidentiality (str, optional): The confidentiality of the dataset. Defaults to 'Confidential'.
            bio_quickscan (str, optional): The BIO quickscan status of the dataset. Defaults to 'Done'.
            privacy (str, optional): The privacy of the dataset. Defaults to 'Niet persoonlijk identificeerbaar'.
            privacy_quickscan (str, optional): The privacy quickscan status of the dataset. Defaults to 'Done'.
            geo_data (str, optional): The geo data of the dataset. Defaults to ''.
            history_start (str, optional): The history start of the dataset. Defaults to ''.
            refresh_rate (str, optional): The refresh rate of the dataset. Defaults to ''.
            end_date (str, optional): The end date of the dataset. Defaults to ''.
            write_to_landingzone (bool, optional): Whether to write the datacontract to the landingzone. Defaults to True.
            output_path (str, optional): The path to write the datacontract to. Defaults to f'/Volumes/{self.catalog}/default/landingzone/datacontracten/{self.dataset}.json'.

        Returns:
            str: The JSON representation of the datacontract.
        """
        # easier with pandas
        pandas_df = self.information_schema.toPandas()
        tables = [
            {
                "name": table_name,
                "attributes": [
                    {
                        row['column_name']: {
                            "type": self._map_types_datacontract(row['full_data_type']),
                            "description": row.get('comment', f"Description of {row['column_name']}"),
                            "term": row.get('tag_value', row['column_name'])
                        }
                    } for row in group.to_dict('records')
                ]
            } for table_name, group in pandas_df.groupby('table_name')
        ]

        data_contract = {
            "Name": self.dataset,
            "Version": version,
            "Description": description,
            "Business goal": business_goal,
            "Theme": theme,
            "Collection": collection,
            "Data Team": datateam,
            "Data Product Owner": product_owner,
            "Data Steward": data_steward,
            "Language": language,
            "Confidentiality": confidentiality,
            "BIO Quickscan": bio_quickscan,
            "Privacy": privacy,
            "Privacy Quickscan": privacy_quickscan,
            "Geo Data": geo_data,
            "History Start": history_start,
            "Refresh Rate": refresh_rate,
            "End Date": end_date,
            "Schema": {"Tables": tables}
        }

        json_data_contract = json.dumps(data_contract, indent=2)

        if write_to_landingzone:
            if output_path is None:
                output_path = f'/Volumes/{self.catalog}/default/landingzone/datacontracten/{self.dataset}.json'

            try:
                with open(output_path, "w") as file:
                    file.write(json_data_contract)
            except Exception as e:
                print(f"Error writing datacontract to {output_path}: {e}")
                print('Vergeet niet om de folder datacontracten aan te maken in de landingzone')
            else:
                print(f"Datacontract written to {output_path}")

        return json_data_contract

    
    def get_tables_dict(self):
        """
        Retrieves a dictionary of table names and their corresponding schemas.

        Returns:
            dict: A dictionary of table names and their corresponding schemas.
        """
        # Use defaultdict to automatically handle missing keys
        tables_dict = defaultdict(list)

        # Collect the DataFrame as a list of rows and directly create a dictionary
        for row in self.information_schema.collect():
            tables_dict[row['table_name']].append((row['column_name'], row['full_data_type']))
        
        return tables_dict

    def _get_column_occurrences(self):
        """
        Retrieves a dictionary of column names and the tables where they occur.

        Returns:
            dict: A dictionary of column names and the tables where they occur.
        """
        occurrences = {}
        for table, schema in self.tables_dict.items():
            for c, data_type in schema:
                occurrences.setdefault(c, []).append(table)
        return occurrences
    
    def _get_relations(self):
        """
        Retrieves a list of relations between columns in different tables.

        TODO
        We should actually be getting this from the information_schema.table_constraints table!

        Returns:
            list: A list of relations between columns in different tables.
        """
        # First get occurrences
        if not hasattr(self, 'occurrences'):
            self.occurrences = self._get_column_occurrences()

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


    def print_eraser_code(self, include_relations=False):
        """
        Prints out the code for creating an Eraser diagram for the dataset.

        Args:
            include_relations (bool, optional): Whether to include relations between columns in the diagram. Defaults to False.
        """
        # Print table definitions
        for table_name, columns in self.tables_dict.items():
            print(f"{table_name} {{")
            [print(f"    {c} {dtype}") for c, dtype in columns]
            print("}\n")

        if include_relations:
            # Print relations
            if not hasattr(self, 'relations'):
                self.relations = self._get_relations()
            
            for c1, c2, r_type in self.relations:
                print(f"{c1} {r_type} {c2}")
            

    def print_drawio_code(self):
        """
        Prints out the code for creating a Draw.io diagram for the dataset.
        """
        for table_name, schema in self.tables_dict.items():
            print('CREATE TABLE', table_name)
            for c in schema:
                print(c)
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
            for table_name in self.tables_dict.keys():
                print(table_name)
                print(self.get_table_stats(table_name).to_markdown())
                print()
        else:
            print(table_name)
            print(self.get_table_stats(table_name).to_markdown())
            print()

# COMMAND ----------

# # Example usage 
# CATALOG = 'dpms_dev'
# SCHEMA = 'gold'
# DATASET = 'amis'

# dataset = Dataset(
#     spark_session=spark,
#     catalog=CATALOG,
#     schema=SCHEMA,
#     dataset=DATASET,
# )