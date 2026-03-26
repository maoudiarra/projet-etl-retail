import logging

from src.extract import extract_data
from src.analyse import analyse_data
from src.transform import (
    handle_missing_values,
    convert_date_format,
    remove_duplicates,
    remove_outliers_iqr,
    add_revenue_column,
    normalize_revenue,
    aggregate_by_product
)
from src.validate import validate_data


# ---------------------------------------------------
# CONFIGURATION DU LOGGING
# ---------------------------------------------------
logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------------------------------------------
# PIPELINE ETL COMPLET
# ---------------------------------------------------
def main():
    logging.info("===== DÉMARRAGE DU PIPELINE ETL =====")

    try:
        # 1️⃣ EXTRACTION
        df = extract_data("data/jeux_de_données.csv")
        print("✅ Extraction réussie")

        print("\n🔎 ANALYSE AVANT TRAITEMENT")
        analyse_data(df)

        # 2️⃣ NETTOYAGE
        df = handle_missing_values(df)
        df = convert_date_format(df)
        df = remove_duplicates(df)

        # 3️⃣ GESTION DES VALEURS ABERRANTES
        df = remove_outliers_iqr(df, "Quantite_vendue")
        df = remove_outliers_iqr(df, "Prix_unitaire")

        # 4️⃣ TRANSFORMATIONS
        df = add_revenue_column(df)
        df = normalize_revenue(df)

        print("\n🔎 ANALYSE APRÈS TRANSFORMATION")
        analyse_data(df)

        # 5️⃣ VALIDATION CROISÉE
        validate_data(df)
        print("✅ Validation des données réussie")

        # 6️⃣ AGRÉGATION
        df_agg = aggregate_by_product(df)

        print("\n📊 AGRÉGATION PAR PRODUIT")
        print(df_agg.head())

        # 7️⃣ EXPORT FINAL
        df.to_csv("data/cleaned_data.csv", index=False)
        df_agg.to_csv("data/aggregated_data.csv", index=False)

        print("\n💾 Données exportées avec succès")

        logging.info("Pipeline exécuté avec succès")

    except Exception as e:
        logging.critical(f"Erreur critique : {e}")
        print("❌ Erreur détectée :", e)

    finally:
        logging.info("===== FIN DU PIPELINE ETL =====\n")


# ---------------------------------------------------
# POINT D'ENTRÉE
# ---------------------------------------------------
if __name__ == "__main__":
    main()