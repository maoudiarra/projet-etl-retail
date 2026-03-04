# 📊 Projet ETL – Retail Data Pipeline

## 🎯 Objectif

Développement d’un pipeline ETL complet en Python dans le cadre du module :

**Conception de Pipelines – Transform**

Le projet consiste à nettoyer, transformer et valider un jeu de données de ventes contenant :

- Valeurs manquantes
- Valeurs aberrantes
- Données incohérentes

---

## ⚙️ Technologies utilisées

- Python 3.11
- Pandas
- NumPy
- Logging
- Git / GitHub

---

## 🏗️ Architecture du projet



---

## 🔄 Étapes du pipeline

### 1️⃣ Analyse des données
- Identification des valeurs manquantes
- Détection des doublons
- Analyse statistique

### 2️⃣ Nettoyage
- Remplacement des valeurs manquantes
- Suppression des doublons
- Détection et suppression des valeurs aberrantes (méthode IQR)

### 3️⃣ Transformations
- Conversion des dates
- Création de la colonne `Chiffre_affaires`
- Normalisation des données
- Agrégation par produit

### 4️⃣ Validation croisée
- Vérification absence de valeurs manquantes
- Vérification prix et quantités positifs
- Vérification type des dates

### 5️⃣ Gestion des erreurs
- Try / Except global
- Logging automatique des erreurs

---

## ▶️ Exécution du projet

```bash
python main.py