import logging
import pandas as pd


def validate_data(df: pd.DataFrame) -> None:
    """
    Vérifications qualité après transformation
    """

    logging.info("Validation des données")

    # 1️⃣ Vérifier absence de valeurs manquantes
    assert df.isnull().sum().sum() == 0, "Il reste des valeurs manquantes"

    # 2️⃣ Vérifier prix positif
    assert (df["Prix_unitaire"] >= 0).all(), "Prix négatif détecté"

    # 3️⃣ Vérifier quantité positive
    assert (df["Quantite_vendue"] >= 0).all(), "Quantité négative détectée"

    # 4️⃣ Vérifier date valide
    assert df["Date_vente"].dtype == "datetime64[ns]", "Date mal convertie"

    logging.info("Validation réussie")