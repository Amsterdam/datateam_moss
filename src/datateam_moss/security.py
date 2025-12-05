import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()


# Functie om kolommen te versleutelen met AES
def encrypt_columns_aes(_df, column_names, encryption_key, deterministic=False, encrypted_column_name_postfix="_encrypted", keep_original_columns=False):
    """
    Encrypts (and encodes) the specified columns in a PySpark DataFrame using AES encryption.

    :param _df: Input DataFrame
    :param column_names: List of names of the columns to encrypt
    :param encryption_key: Secret key for AES encryption (16, 24, or 32 bytes)
    :param deterministic: Whether to use deterministic encryption or not
    :param encrypted_column_name_postfix: Postfix to append to the original column name for the new column
    :param keep_original_columns: Whether to keep the original columns
    :return: DataFrame with the encrypted column
    """
    mode = 'ECB' if deterministic else 'GCM'
    padding = 'DEFAULT'

    # Add the encrypted columns to the DataFrame
    for column_name in column_names:
        # Use the built-in `aes_encrypt` function in Databricks SQL
        # we encode the value as a base64 string to be able to store and transport it
        encrypted_column_expr = f"base64(aes_encrypt({column_name}, '{encryption_key}', '{mode}', '{padding}'))"
        _df = _df.withColumn(column_name + encrypted_column_name_postfix, F.expr(encrypted_column_expr))

    columns_to_drop = [] if keep_original_columns else column_names
    return _df.drop(*columns_to_drop)

def decrypt_columns_aes(_df: DataFrame, column_names: F.Iterable[str], encryption_key: bytes, deterministic: bool = False, decrypted_column_name_postfix: str = "_decrypted", keep_original_columns: bool = False):
    """
    Decrypts (and decodes) the specified columns in a PySpark DataFrame using AES encryption.

    :param _df: Input DataFrame
    :param column_names: List of names of the columns to decrypt
    :param encryption_key: Secret key used for AES encryption (16, 24, or 32 bytes)
    :param deterministic: Whether deterministic encryption is used or not
    :param encrypted_column_name_postfix: Postfix to append to the original column name for the new column
    :param keep_original_columns: Whether to keep the original columns
    :return: DataFrame with the encrypted column
    """

    mode = 'ECB' if deterministic else 'GCM'
    padding = 'DEFAULT'

    # Add the encrypted columns to the DataFrame
    for column_name in column_names:
        # Use the built-in `aes_decrypt` function in Databricks SQL
        # we need to decode the base64 string first
        # we also need to cast the result to STRING as AES_DECRYPT returns a BINARY
        decrypted_column_expr = f"CAST(aes_decrypt(unbase64({column_name}), '{encryption_key}', '{mode}', '{padding}') AS STRING)"
        _df = _df.withColumn(column_name+decrypted_column_name_postfix, F.expr(decrypted_column_expr))

    columns_to_drop = [] if keep_original_columns else column_names
    return _df.drop(*columns_to_drop)

# Functie om een masker toe te voegen aan Databricks
def add_databricks_mask_function(
    catalog: str,
    schema: str,
    function_name: str = "mask_column",
    groups_with_access: list[str] | None = None,
    spns_with_access: list[str] | None = None,
    mask_str: str = '*****'
) -> None:
    """
    Creates or replaces a masking function in a Databricks schema.

    The function returns the input string if the current user is a member of any of the
    specified account groups or matches any of the specified service principals (SPNs).
    Otherwise, it returns a masked string.

    Parameters:
        catalog (str): The Databricks catalog where the function will be created.
        schema (str): The schema within the catalog where the function will reside.
        function_name (str): Name of the masking function to create. Defaults to 'mask_column'.
        groups_with_access (list[str] | None): Account group names whose members can see unmasked data. Defaults to None.
        spns_with_access (list[str] | None): User identifiers (client IDs) of service principals allowed to see unmasked data. Defaults to None.
        mask_str (str): String to return when masking is applied. Defaults to '*****'.

    Returns:
        None

    Raises:
        ValueError: If neither `groups_with_access` nor `spns_with_access` is provided.

    """

    access_conditions = []

    if groups_with_access:
        for group in groups_with_access:
            access_conditions.append(f"is_account_group_member('{group}')")

    if spns_with_access:
        for spn in spns_with_access:
            access_conditions.append(f"current_user() = '{spn}'")

    if not access_conditions:
        raise ValueError("At least one group or SPN with access must be provided.")

    access_conditions_sql = " OR ".join(access_conditions)

    sql_command = f"""
        CREATE OR REPLACE FUNCTION {catalog}.{schema}.{function_name}(input_str STRING)
        RETURNS STRING
        RETURN CASE 
            WHEN {access_conditions_sql} THEN input_str
            ELSE '{mask_str}'
        END
    """

    spark.sql(sql_command)

# Functie om maskering toe te passen op een specifieke kolom
def apply_databricks_mask_function_to_column(catalog: str, schema: str, policy_schema: str, table: str, column_to_mask: str) -> None:
    """
    Applies the `mask_column` function to a specified column in a Databricks table.

    This function modifies the specified table by applying the pre-defined masking function
    (`mask_column`) to the designated column. The column will return masked values for users 
    who do not belong to the specified group with access.

    Parameters:
        catalog (str): The Databricks catalog where the table resides.
        schema (str): The schema within the catalog where the table resides.
        policy_schema (str): The schema where the policy resides.
        table (str): The name of the table to apply the mask to.
        column_to_mask (str): The column in the table that will be masked.

    Returns:
        None
    """

    sql_command = f"""
        ALTER TABLE {catalog}.{schema}.{table}
        ALTER COLUMN {column_to_mask} SET MASK {catalog}.{policy_schema}.mask_column
        """
    spark.sql(sql_command)