# Databricks notebook source

import pandas as pd
import numpy as np
import re

def label(categorie: str, kolommen: pd.DataFrame, strings: list, strings_not = []) -> pd.Series:
    """
    Labelt de rijen van een DataFrame op basis van categorieën en matchende strings.

    Args:
        categorie (str): De naam van de categorie om toe te passen op de matches.
        kolommen (pd.DataFrame): De DataFrame-kolommen die doorzocht moeten worden.
        strings (list): Een lijst van strings die gematcht moeten worden binnen de kolommen.
        strings_not (list): Een list die, indien hij elementen bevat, gebruikt wordt als NOT om geen categorisatie toe te passen bij een match

    Returns:
        pd.Series: Een Pandas Series met de categorieën toegewezen aan de juiste rijen.
    """

    # Geef categorie een hoofdletter en vervang underscores door spaties
    categorie = categorie.capitalize().replace('_', ' ')
    
    # Controleer of strings niet leeg is om onnodige regex-operaties te voorkomen
    if not strings:
        return pd.Series([''] * len(kolommen), index=kolommen.index)
    
    # Maak een regex-patroon met de OR-operator tussen de strings, escape indien nodig
    r = '|'.join(map(re.escape, strings))
    
    # Aggregeer de kolommen naar één string om te controleren
    if kolommen.shape[1] > 1:
        check_tekst = kolommen.astype(str).agg(''.join, axis=1)
    else:
        check_tekst = kolommen.iloc[:, 0].astype(str)
    
    # Pas not toe indien strings zijn opgegeven
    if len(strings_not) > 0:
        r_not = '|'.join(map(re.escape, strings_not))
        # Controleer per string not en daarna per string of deze in de regex voorkomt en label met de categorie
        output = check_tekst.apply(lambda x: '' if re.search(r_not, x)  else (categorie if re.search(r, x) else ''))

    else:
        # Controleer per string of deze in de regex voorkomt en label met de categorie
        output = check_tekst.apply(lambda x: categorie if pd.notnull(x) and re.search(r, x) else '')
    
    return output

def label_dataset(df: pd.DataFrame, dictionary: dict, kolomnaam_output: str, kolom_per_categorie: bool = False) -> pd.DataFrame:
    """
    Labelt een DataFrame op basis van opgegeven categorieën en patronen.

    Args:
        df (pd.DataFrame): Invoer DataFrame dat gelabeld moet worden.
        dictionary (dict): Een dictionary waarin de sleutels de categorieën zijn en de waarden lijsten bevatten met:
                           1) Kolommen waarin gezocht moet worden.
                           2) Strings om te matchen.
        kolomnaam_output (str): De naam van de uitvoerkolom die de geaggregeerde categorieën zal bevatten.
        kolom_per_categorie (bool, optioneel): Als True, maakt het een kolom per categorie; anders aggregeert het alle categorieën in één kolom.

    Returns:
        pd.DataFrame: Het gelabelde DataFrame.
    """

    # Controleer dat df een pandas DataFrame is
    if not isinstance(df, pd.DataFrame):
        raise TypeError('De invoer moet een Pandas DataFrame zijn met één of meerdere kolommen.')

    # Controleer dat dictionary een geldige dict is
    if not isinstance(dictionary, dict):
        raise TypeError('Categorisatie moet worden opgegeven als een dictionary met categorieën als sleutels en een lijst met kolomnamen en strings als waarden.')

    # Controleer dat kolomnaam_output een string is
    if not isinstance(kolomnaam_output, str):
        raise TypeError('De naam van de uitvoerkolom moet een string zijn.')

    # Label elke categorie in de dictionary
    for k,v in dictionary.items():
        categorie = k
        kolommen = v[0]
        strings = v[1]
        #check of er een derde list met strings_not is
        if len(v) == 3:
            strings_not = v[2]
            df[categorie] = label(categorie=categorie, kolommen=df[kolommen], strings=strings,strings_not=strings_not)
        else:
            df[categorie] = label(categorie=categorie, kolommen=df[kolommen], strings=strings)

    # Aggregeer naar een enkele uitvoerkolom als kolom_per_categorie False is
    if not kolom_per_categorie:
        categorieen = dictionary.keys()
        df[kolomnaam_output] = df[categorieen].agg(list,axis=1).apply(lambda x: [i for i in x if i != ''])
        df = df.drop(columns=list(dictionary.keys()))

    return df
