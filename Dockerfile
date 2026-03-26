# Image de base
FROM python:3.11-slim

# Dossier de travail
WORKDIR /app

# Copier les fichiers
COPY . /app

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Commande d'exécution
CMD ["python", "etl_pipeline.py"]