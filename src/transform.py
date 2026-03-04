import logging
import pandas as pd


# ---------------------------------------------------
# 1️⃣ TRAITEMENT DES VALEURS MANQUANTES
# ---------------------------------------------------
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Traitement des valeurs manquantes")

    if df["Quantite_vendue"].isnull().sum() > 0:
        median_value = df["Quantite_vendue"].median()
        df["Quantite_vendue"] = df["Quantite_vendue"].fillna(median_value)

    if df["Prix_unitaire"].isnull().sum() > 0:
        mean_value = df["Prix_unitaire"].mean()
        df["Prix_unitaire"] = df["Prix_unitaire"].fillna(mean_value)

    if df["Nom_produit"].isnull().sum() > 0:
        df["Nom_produit"] = df["Nom_produit"].fillna("Produit_inconnu")

    return df


# ---------------------------------------------------
# 2️⃣ CONVERSION DATE
# ---------------------------------------------------
def convert_date_format(df: pd.DataFrame) -> pd.DataFrame:
    df["Date_vente"] = pd.to_datetime(df["Date_vente"], errors="coerce")
    return df


# ---------------------------------------------------
# 3️⃣ SUPPRESSION DES DOUBLONS
# ---------------------------------------------------
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Suppression des doublons")
    before = df.shape[0]
    df = df.drop_duplicates()
    after = df.shape[0]
    logging.info(f"{before - after} doublons supprimés")
    return df


# ---------------------------------------------------
# 4️⃣ GESTION DES OUTLIERS (IQR)
# ---------------------------------------------------
def remove_outliers_iqr(df: pd.DataFrame, column: str) -> pd.DataFrame:
    logging.info(f"Détection des outliers sur {column}")

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    before = df.shape[0]

    df = df[(df[column] >= lower) & (df[column] <= upper)]

    after = df.shape[0]
    logging.info(f"{before - after} outliers supprimés pour {column}")

    return df


# ---------------------------------------------------
# 5️⃣ AJOUT CHIFFRE D'AFFAIRES
# ---------------------------------------------------
def add_revenue_column(df: pd.DataFrame) -> pd.DataFrame:
    df["Chiffre_affaires"] = df["Quantite_vendue"] * df["Prix_unitaire"]
    return df


# ---------------------------------------------------
# 6️⃣ NORMALISATION
# ---------------------------------------------------
def normalize_revenue(df: pd.DataFrame) -> pd.DataFrame:
    min_val = df["Chiffre_affaires"].min()
    max_val = df["Chiffre_affaires"].max()

    df["CA_normalise"] = (
        (df["Chiffre_affaires"] - min_val) / (max_val - min_val)
    )

    return df


# ---------------------------------------------------
# 7️⃣ AGRÉGATION PAR PRODUIT
# ---------------------------------------------------
def aggregate_by_product(df: pd.DataFrame) -> pd.DataFrame:
    df_agg = (
        df.groupby("Nom_produit")["Chiffre_affaires"]
        .sum()
        .reset_index()
    )

    return df_agg