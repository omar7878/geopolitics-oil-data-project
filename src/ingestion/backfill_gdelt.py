import requests
import zipfile
import io
import boto3
import os
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
S3_ENDPOINT = "http://localhost:4566"
BUCKET_NAME = "datalake"
MASTER_FILE_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

# Identifiants LocalStack
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT)

def run_backfill(days_back=7): # Par défaut, on teste sur 7 jours
    print(f"📂 Récupération de l'historique GDELT (Master File List)...")
    
    try:
        # 1. Télécharger la liste de tous les fichiers existants
        r = requests.get(MASTER_FILE_LIST_URL)
        r.raise_for_status()
        
        # 2. Parser le fichier (Espace comme séparateur)
        lines = [line.split(' ') for line in r.text.strip().split('\n')]
        df = pd.DataFrame(lines, columns=['size', 'hash', 'url'])
        
        # 3. Extraire et filtrer par date
        # GDELT utilise des dates à 14 chiffres dans l'URL : YYYYMMDDHHMMSS
        df['dt_str'] = df['url'].str.extract(r'/(\d{14})\.')
        df['datetime'] = pd.to_datetime(df['dt_str'], format='%Y%m%d%H%M%S', errors='coerce')
        
        start_date = datetime.now() - timedelta(days=days_back)
        
        # On ne garde que les "export" (les événements) postérieurs à start_date
        to_process = df[
            (df['url'].str.contains('.export.CSV.zip')) & 
            (df['datetime'] >= start_date)
        ].sort_values('datetime')

        print(f"🚀 {len(to_process)} fichiers à rattraper pour les {days_back} derniers jours.")

        # 4. Boucle d'ingestion
        for idx, row in to_process.iterrows():
            url = row['url']
            date_folder = row['datetime'].strftime('%Y-%m-%d')
            
            # Téléchargement
            zip_r = requests.get(url)
            
            with zipfile.ZipFile(io.BytesIO(zip_r.content)) as z:
                for filename in z.namelist():
                    s3_key = f"raw/gdelt/{date_folder}/{filename}"
                    
                    # Upload direct
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=s3_key,
                        Body=z.read(filename)
                    )
                    print(f"✅ Ingested: {s3_key}")

        print(f"✨ Backfill de {days_back} jours terminé !")

    except Exception as e:
        print(f"❌ Erreur critique : {e}")

if __name__ == "__main__":
    # CONSEIL : Teste d'abord avec 1 ou 2 jours avant de mettre 180 !
    run_backfill(days_back=2)