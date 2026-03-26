"""
Test de l'API publique — JSONPlaceholder
Endpoint utilisé dans le mini-projet ETL CPPE835

Exécuter ce script pour vérifier que l'API est accessible
et comprendre la structure des données retournées.

Installation : pip install requests
"""

import requests
import json

API_URL = "https://jsonplaceholder.typicode.com/users"

print("=" * 50)
print("TEST API — JSONPlaceholder /users")
print("=" * 50)

try:
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    print(f"\n✅ Statut HTTP : {response.status_code}")
    print(f"✅ Nombre d'utilisateurs retournés : {len(data)}")
    print(f"\n📋 Clés disponibles dans chaque objet : {list(data[0].keys())}")
    print(f"\n📄 Exemple (premier utilisateur) :")
    print(json.dumps(data[0], indent=2))

    print("\n💡 Pour le projet : les customer_id de 1 à 10 seront considérés 'premium'")
    print("   (car l'API retourne 10 utilisateurs avec id de 1 à 10)")

except requests.exceptions.ConnectionError:
    print("❌ Impossible de joindre l'API (vérifiez votre connexion internet)")
except requests.exceptions.Timeout:
    print("❌ Timeout — l'API met trop de temps à répondre")
except requests.exceptions.HTTPError as e:
    print(f"❌ Erreur HTTP : {e}")
