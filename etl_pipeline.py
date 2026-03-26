import pandas as pd
import requests
import sqlite3
from datetime import datetime
import logging
import os

# ==========================
# CONFIG LOG
# ==========================
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==========================
# EXTRACT
# ==========================

def extract_api():
    try:
        url = "https://jsonplaceholder.typicode.com/users"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logging.info(f"API: {len(df)} lignes extraites")
        return df
    except Exception as e:
        logging.error(f"Erreur API: {e}")
        return pd.DataFrame()


def extract_sqlite():
    try:
        conn = sqlite3.connect("customers.db")
        query = "SELECT customer_id, email, signup_date, country FROM customers"
        df = pd.read_sql(query, conn)
        conn.close()
        logging.info(f"SQLite: {len(df)} lignes extraites")
        return df
    except Exception as e:
        logging.error(f"Erreur SQLite: {e}")
        return pd.DataFrame()


def extract_csv():
    try:
        df = pd.read_csv("orders.csv")
        logging.info(f"CSV: {len(df)} lignes extraites")

        print("\n=== INFO CSV ===")
        print(df.info())
        print("\n=== NULL VALUES ===")
        print(df.isnull().sum())

        return df
    except Exception as e:
        logging.error(f"Erreur CSV: {e}")
        return pd.DataFrame()


# ==========================
# TRANSFORM
# ==========================

def transform(df_orders, df_customers, df_api):

    # 🔹 Nettoyage
    df_orders = df_orders.dropna()  # simple stratégie

    # 🔹 Dates
    df_orders["order_date"] = pd.to_datetime(df_orders["order_date"])
    df_customers["signup_date"] = pd.to_datetime(df_customers["signup_date"])

    # 🔹 Jointure
    df = df_orders.merge(df_customers, on="customer_id", how="left")

    # 🔹 Enrichissement
    df["days_since_signup"] = (df["order_date"] - df["signup_date"]).dt.days

    # 🔹 Premium (API id 1-10)
    premium_ids = df_api["id"].tolist()
    df["is_premium"] = df["customer_id"].isin(premium_ids)

    # 🔹 Timestamp ETL
    df["etl_timestamp"] = datetime.now()

    # 🔹 Validation
    df = df.dropna(how='all')

    print("\n=== DATA FINAL ===")
    print(df.head())

    logging.info(f"Transform: {len(df)} lignes après transformation")

    return df


# ==========================
# LOAD
# ==========================

def load(df):

    # 🔹 Connexion DB
    conn = sqlite3.connect("datawarehouse.db")
    cursor = conn.cursor()

    # 🔹 Création table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_orders_enriched (
        order_id INTEGER,
        customer_id INTEGER,
        product_name TEXT,
        amount REAL,
        order_date TEXT,
        email TEXT,
        signup_date TEXT,
        country TEXT,
        days_since_signup INTEGER,
        is_premium BOOLEAN,
        etl_timestamp TEXT
    )
    """)

    # 🔹 TRUNCATE
    cursor.execute("DELETE FROM fact_orders_enriched")

    # 🔹 INSERT
    df.to_sql("fact_orders_enriched", conn, if_exists='append', index=False)

    conn.commit()
    conn.close()

    logging.info(f"Load DB: {len(df)} lignes insérées")

    # ==========================
    # EXPORT CSV HISTORISÉ
    # ==========================

    if not os.path.exists("output"):
        os.makedirs("output")

    filename = f"output/fact_orders_enriched_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)

    logging.info(f"CSV exporté: {filename}")

    print(f"\n✅ Données chargées + CSV généré: {filename}")


# ==========================
# MAIN
# ==========================

if __name__ == "__main__":

    print("\n🚀 Lancement du pipeline ETL...\n")

    df_api = extract_api()
    df_customers = extract_sqlite()
    df_orders = extract_csv()

    df_final = transform(df_orders, df_customers, df_api)

    load(df_final)

    print("\n✅ Pipeline terminé avec succès !")