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
import pandas as pd
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

# Colonnes GDELT v2 Events (58 colonnes)
GDELT_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
    "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID", "ActionGeo_Type",
    "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long",
    "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL"
]

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

        # 3. Extraction, conversion en Parquet et Upload vers S3
        with zipfile.ZipFile(io.BytesIO(zip_r.content)) as z:
            for filename in z.namelist():
                # On définit la partition du jour : YYYY-MM-DD
                today = datetime.now().strftime('%Y-%m-%d')
                
                # Lire le CSV en DataFrame
                with z.open(filename) as csv_file:
                    events_df = pd.read_csv(
                        csv_file,
                        sep='\t',
                        header=None,
                        names=GDELT_COLUMNS,
                        dtype=str
                    )
                
                # Convertir en Parquet
                parquet_filename = filename.replace('.CSV', '.parquet').replace('.csv', '.parquet')
                parquet_buffer = io.BytesIO()
                events_df.to_parquet(
                    parquet_buffer, index=False, engine='pyarrow',
                    coerce_timestamps='us', allow_truncated_timestamps=True,
                )
                parquet_buffer.seek(0)
                
                # Chemin final : raw/gdelt/daily/2026-02-24/nom_du_fichier.parquet
                s3_key = f"raw/gdelt/daily/{today}/{parquet_filename}"
                
                print(f"🚀 Upload de {parquet_filename} vers s3://{BUCKET_NAME}/{s3_key}...")
                
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                    Body=parquet_buffer.getvalue()
                )
        
        print("✅ Ingestion terminée avec succès !")

    except Exception as e:
        print(f"❌ Erreur lors de l'ingestion : {e}")

if __name__ == "__main__":
    run_gdelt_ingestion()