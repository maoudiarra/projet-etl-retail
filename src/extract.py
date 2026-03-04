import pandas as pd
import logging

def extract_data(path: str) -> pd.DataFrame:
    """
    Lit un fichier CSV et retourne un DataFrame pandas.
    """
    try:
        logging.info(f"Lecture du fichier : {path}")
        df = pd.read_csv(path)
        logging.info("Extraction réussie")
        return df

    except FileNotFoundError:
        logging.error("Fichier introuvable.")
        raise

    except pd.errors.EmptyDataError:
        logging.error("Fichier vide.")
        raise

    except Exception as e:
        logging.error(f"Erreur inattendue : {e}")
        raise