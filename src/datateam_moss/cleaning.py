# Databricks notebook source
def clean_column_names(cols):
    """
    Clean and standardize column names by converting to lowercase, replacing unwanted characters with underscores,
    collapsing consecutive underscores, and removing leading/trailing whitespaces and underscores.

    Otherwise, spark will throw this error on Unity Catalog:
        `Found invalid character(s) among ' ,;{}()\n\t=' in the column names of your schema.`

    Args:
        cols (list): List of column names to be cleaned.

    Returns:
        list: List of cleaned column names.

    Example:
        test_strings = [" First Name ", "_Last_Name_", "__Email-Address__", "Liquiditeit (current ratio)", "fte's mannen 2021?", "2017/'18*"]
        clean_column_names(test_strings)

    Usage:
        # spark
        df = df.toDF(*clean_column_names(df.columns))

        # polars
        df.columns = clean_column_names(df.columns)
    """
    # Unwanted
    UNWANTED_CHARS = r"[ ,;{}:()\n\t=\.\-/'?\*]"
    cleaned_cols = [re.sub(UNWANTED_CHARS, '_', col.lower()).replace('___', '_').replace('__', '_').strip('_').strip() for col in cols]
    return cleaned_cols
    
def clean_dataframe(df: DataFrame):
    """
    Clean and standardize dataframe column names by converting to lowercase, replacing unwanted characters with underscores,
    collapsing consecutive underscores, and removing leading/trailing whitespaces and underscores.

    Otherwise, spark will throw this error on Unity Catalog:
        `Found invalid character(s) among ' ,;{}()\n\t=' in the column names of your schema.`

    Args:
        df (pyspark.sql.DataFrame): DataFrame to be cleaned.

    Returns:
        pyspark.sql.DataFrame: Cleaned DataFrame.
    """
    return df.toDF(*clean_column_names(df.columns))

def normalize_date_string(date_str):
    """Normaliseert een datumstring door voorloopnullen toe te voegen aan dag-, maand-, uur-, minuut- en secondecomponenten.

    Deze functie past dag-, maand- en tijdcomponenten in een datumstring aan door voorloopnullen toe te voegen
    waar nodig, zodat alle enkelvoudige cijfers worden weergegeven als twee cijfers. Dit is vooral nuttig bij 
    inconsistenties in datumformaten.

    Args:
        date_str (str): De te normaliseren datumstring, bijvoorbeeld '1-1-1980 0:0:0' of '1-jan-1980 0:0:0'.

    Returns:
        str: De genormaliseerde datumstring, bijvoorbeeld '01-01-1980 00:00:00', of None als de invoer leeg is.
    """
    if date_str is None:
        return None
    
    # Normaliseer de dag (1 naar 01) en maand (1 naar 01)
    date_str = re.sub(r'\b(\d{1})\b', r'0\1', date_str)
    
    # Voeg voorloopnullen toe aan uur:min:sec notatie indien nodig
    date_str = re.sub(r'(\d{2}-\d{2}-\d{4}) (\d{1}):(\d{1}):(\d{1})', r'\1 0\2:0\3:0\4', date_str)
    date_str = re.sub(r'(\d{2}-\d{2}-\d{4}) (\d{1}):(\d{1}):(\d{2})', r'\1 0\2:0\3:\4', date_str)
    date_str = re.sub(r'(\d{2}-\d{2}-\d{4}) (\d{2}):(\d{1}):(\d{1})', r'\1 \2:0\3:0\4', date_str)
    
    # Herken maandnamen en vervang deze door een numerieke maandindeling (optioneel)
    month_map = {
        "jan": "01", "feb": "02", "mrt": "03", "apr": "04",
        "mei": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "okt": "10", "nov": "11", "dec": "12"
    }
    for month_name, month_num in month_map.items():
        date_str = re.sub(rf'-{month_name}-', f'-{month_num}-', date_str, flags=re.IGNORECASE)
    
    return date_str

# Definieer een UDF voor normalisatie
normalize_date_string_udf = F.udf(normalize_date_string, T.StringType())

def parse_and_format_date(df, date_column, output_format="yyyy-MM-dd"):
    """Parseert en formatteert een datumstringkolom naar een uniform formaat.

    Deze functie probeert verschillende datumformaten te herkennen in een kolom met datumstrings, 
    normaliseert de waarden (voegt voorloopnullen toe) en converteert deze naar een uniform `yyyy-MM-dd` formaat,
    of een andere opgegeven uitvoerindeling.

    Args:
        df (pyspark.sql.DataFrame): Het invoer-DataFrame met de datumkolom.
        date_column (str): De naam van de kolom met datumstrings die moeten worden geparseerd.
        output_format (str, optioneel): Het gewenste uitvoerformaat, standaard is 'yyyy-MM-dd'.

    Returns:
        pyspark.sql.DataFrame: Het DataFrame met de toegevoegde en geformatteerde datumkolom.
    """
    # Mogelijke datumformaten
    possible_date_formats = [
        "dd-MM-yyyy HH:mm:ss",  # 01-01-1980 00:00:00
        "dd-MM-yyyy",           # 01-01-1980
        "yyyy-MM-dd",           # 1980-01-01
        "MM/dd/yyyy",           # 01/01/1980
        "dd-MMM-yyyy HH:mm:ss", # 01-jan-1980 00:00:00
        "dd-MMM-yyyy"           # 01-jan-1980
    ]
    
    # Normaliseer eerst de datumstring met voorloopnullen
    df = df.withColumn("normalized_date", normalize_date_string_udf(F.col(date_column)))
    
    # Probeer elk format en neem de eerste die niet null is
    parsed_column = None
    for fmt in possible_date_formats:
        if parsed_column is None:
            parsed_column = F.to_date(F.unix_timestamp("normalized_date", fmt).cast("timestamp"))
        else:
            parsed_column = F.coalesce(parsed_column, F.to_date(F.unix_timestamp("normalized_date", fmt).cast("timestamp")))

    # Voeg de parsed datum toe als kolom
    df = df.withColumn("parsed_date", parsed_column)

    # Format de datum naar het gewenste outputformat
    df = df.withColumn("aangemaakt", F.date_format("parsed_date", output_format).cast("date"))
    
    # Selecteer alleen de gewenste kolommen
    return df.drop("parsed_date", "normalized_date")
