# ============================================================
# batch_extract_gdelt.py
# Étape 1 : Ingestion batch des événements géopolitiques GDELT
#
# Rôle : Interroge l'API GDELT DOC 2.0, récupère les articles
#        liés aux mots-clés géopolitiques (oil, iran, opec...),
#        et sauvegarde le résultat brut en JSON sur S3.
#
# Appelé par : Airflow (toutes les 15 minutes via DAG)
# Output      : s3://datalake/raw/gdelt/YYYY-MM-DD/events.json
# ============================================================

# TODO: Implémenter le script d'extraction GDELT

import requests
import zipfile
import io
import boto3
import os
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
S3_ENDPOINT = "http://localhost:4566"
BUCKET_NAME = "datalake"
GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Identifiants bidons pour LocalStack (nécessaires pour boto3)
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

def run_gdelt_ingestion():
    """
    Récupère le dernier fichier ZIP de GDELT, l'extrait en mémoire,
    et l'envoie sur le S3 local (LocalStack).
    """
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT)
    
    print(f"🌐 Consultation de GDELT pour le dernier fichier disponible...")
    
    try:
        # 1. Récupérer l'URL du dernier fichier ZIP (le premier lien dans lastupdate.txt)
        r = requests.get(GDELT_LAST_UPDATE_URL)
        r.raise_for_status()
        # Le format est : 123456 abcdef... http://...export.CSV.zip
        last_file_url = r.text.split('\n')[0].split(' ')[2]
        print(f"🔗 URL trouvée : {last_file_url}")

        # 2. Téléchargement du ZIP en mémoire (io.BytesIO)
        print("📥 Téléchargement en cours...")
        zip_r = requests.get(last_file_url)
        zip_r.raise_for_status()

        # 3. Extraction et Upload direct vers S3
        with zipfile.ZipFile(io.BytesIO(zip_r.content)) as z:
            for filename in z.namelist():
                # On définit la partition du jour : YYYY-MM-DD
                today = datetime.now().strftime('%Y-%m-%d')
                
                # Chemin final : raw/gdelt/2026-02-24/nom_du_fichier.csv
                s3_key = f"raw/gdelt/{today}/{filename}"
                
                print(f"🚀 Upload de {filename} vers s3://{BUCKET_NAME}/{s3_key}...")
                
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                    Body=z.read(filename)
                )
        
        print("✅ Ingestion terminée avec succès !")

    except Exception as e:
        print(f"❌ Erreur lors de l'ingestion : {e}")

if __name__ == "__main__":
    run_gdelt_ingestion()