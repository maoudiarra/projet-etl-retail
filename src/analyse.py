import logging
import pandas as pd

def analyse_data(df: pd.DataFrame) -> None:
    """
    Analyse exploratoire du dataset.
    """
    logging.info("Début analyse des données")

    print("\n📊 DIMENSIONS DU DATASET")
    print(df.shape)

    print("\n📋 TYPES DES COLONNES")
    print(df.dtypes)

    print("\n❓ VALEURS MANQUANTES")
    print(df.isnull().sum())

    print("\n🔁 DOUBLONS")
    print("Nombre de doublons :", df.duplicated().sum())

    print("\n📈 STATISTIQUES NUMÉRIQUES")
    print(df.describe())

    logging.info("Analyse terminée")