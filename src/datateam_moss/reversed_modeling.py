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
            catalog='catalog_name', # bijv. 'dpms_dev'
            schema='schema_name', # bijv. 'silver'
            project_prefixes=['prefix_'], # bijv. 'sport_'
            exclude=['test'] 
        )
        dataset.print_eraser_code()
        dataset.print_drawio_code()

    Args:
        spark_session (SparkSession): The SparkSession to use, which is already loaded into each databricks notebooks as 'spark'.
        catalog (str): The name of the catalog.
        schema (str): The name of the schema.
        project_prefix (list, optional): A list of project prefixes to filter tables. Defaults to [].
        exclude (list, optional): A list of table names to exclude. Defaults to [].
    """

    def __init__(self, spark_session, catalog: str, schema: str, dataset: str = '', excluded_table_names: list = []):
        self.spark = spark_session
        self.catalog = catalog
        self.schema = schema
        self.dataset = dataset
        self.excluded_table_names = excluded_table_names
        self.information_schema = self.get_information_schema()
        self.tables_dict = self.get_tables_dict()
        # self.occurrences = self.get_column_occurrences()
        # self.relations = self.get_relations()
        # self.type_of_relations = {}

    def get_information_schema(self):
        """
        Retrieves all tables and columns in the schema with dataset_ as prefix to create a datacontract.

        Args:
            dataset (str): The name of the dataset.

        Returns:
            spark.DataFrame: A DataFrame with all columns and types in the tables.
        """
        # df1 = spark.sql(
        #     f"""
        #     select c.table_name name, c.column_name attributename, c.full_data_type type , c.comment description, tg.tag_value term
        #     from {self.catalog}.information_schema.tables t
        #     inner join {self.catalog}.information_schema.columns c on t.table_name = c.table_name and t.table_schema = c.table_schema
        #     left outer join  (select * from {self.catalog}.information_schema.column_tags where tag_name = 'term') tg on tg.table_name = c.table_name and tg.schema_name = c.table_schema

        #     where c.table_schema = '{self.schema}'
        #     and c.table_name LIKE '%{self.dataset}_%'
        #     order by c.table_name,c.ordinal_position
        #     """
        # )

        # python version of the above, first set the pyspark tables
        tables = self.spark.table(f"{self.catalog}.information_schema.tables")
        columns = self.spark.table(f"{self.catalog}.information_schema.columns")
        column_tags = self.spark.table(f"{self.catalog}.information_schema.column_tags").filter("tag_name = 'term'")

        df = (tables
            .join(columns, 
                    (col("tables.table_name") == col("columns.table_name")) & 
                    (col("tables.table_schema") == col("columns.table_schema")), 
                    "inner")
            .join(column_tags, 
                    (col("column_tags.table_name") == col("columns.table_name")) & 
                    (col("column_tags.schema_name") == col("columns.table_schema")), 
                    "left_outer")
            # .filter((col("columns.table_schema") == self.schema) & col("columns.table_name").like(f"%{self.dataset}_%"))
            .filter(
                (col("columns.table_schema") == self.schema) & 
                col("columns.table_name").like(f"%{self.dataset}_%") & 
                ~col("columns.table_name").isin(self.excluded_table_names)
            )

            # we need to select the right columns and order them
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

    def create_datacontract(self, dataset, version='v0', datateam='MOSS', product_owner='x.name@amsterdam.nl', description='Description of the dataset', business_goal='Business goal of the dataset', write_to_landingzone=True):
        # Convert the Spark DataFrame to pandas and prepare the tables part
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

        # Define the data contract with the tables included
        data_contract = {
            "Name": dataset,
            "Version": version,
            "Description": description,
            "Business goal": business_goal,
            "Theme": "X",
            "Collection": "X",
            "Data Team": datateam,
            "Data Product Owner": product_owner,
            "Data Steward": "",
            "Language": "Nederlands",
            "Confidentiality": "Confidential",
            "BIO Quickscan": "Done",
            "Privacy": "Niet persoonlijk identificeerbaar",
            "Privacy Quickscan": "Done",
            "Geo Data": "",
            "History Start": "",
            "Refresh Rate": "",
            "End Date": "",
            "Schema": {"Tables": tables}
        }

        json_data_contract = json.dumps(data_contract, indent=2)

        if write_to_landingzone:
            output_path = f"/Volumes/{self.catalog}/default/landingzone/datacontracten/{dataset}.json"
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
        # use defaultdict to automatically handle missing keys
        tables_dict = defaultdict(list)

        # collect the DataFrame as a list of rows and directly create a dictionary
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
        # first get occurrences
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

    # def print_eraser_code(self):
    #     """
    #     Prints out the full code for creating an ER Diagram in eraser.io for the dataset.
    #     """
    #     for table_name, schema in self.tables_dict.items():
    #         print(table_name, '{')
    #         for c in schema:
    #             print('\t', c.name, c.dataType.simpleString())
    #         print('}')
    #         print()

    #     # print relations
    #     for c1, c2, r_type in self.relations:
    #         print(f' {r_type} '.join((c1, c2)))
    def print_eraser_code(self, include_relations=False):
        # Collect the DataFrame as a list of rows and directly create and print the dictionary
        for table_name, columns in self.tables_dict.items():
            print(f"{table_name} {{")
            [print(f"    {c} {dtype}") for c, dtype in columns]
            print("}\n")


        if include_relations:
            # print relations
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

# COMMAND ----------

CATALOG = 'dpms_dev'
SCHEMA = 'gold'
DATASET = 'amis'

dataset = Dataset(
    spark_session=spark,
    catalog=CATALOG,
    schema=SCHEMA,
    dataset=DATASET,
    excluded_table_names=['test']
)

# COMMAND ----------

dataset.print_eraser_code()

# COMMAND ----------



# COMMAND ----------

r = dataset.create_datacontract('amis', write_to_landingzone=False)
print(r)

# COMMAND ----------

