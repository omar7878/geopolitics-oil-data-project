import requests
import zipfile
import io
import boto3
import os
import pandas as pd
from datetime import datetime, timezone

# --- Configuration ---
S3_ENDPOINT = "http://localhost:4566"
BUCKET_NAME = "datalake"
MASTER_FILE_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

# Identifiants LocalStack
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT)

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

def run_backfill(start_date: datetime = datetime(2026, 1, 4, 0, 0, tzinfo=timezone.utc)):
    print(f"📂 Récupération de l'historique GDELT depuis {start_date} (Master File List)...")
    
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
        df['datetime'] = pd.to_datetime(df['dt_str'], format='%Y%m%d%H%M%S', errors='coerce', utc=True)
        
        # On ne garde que les "export" (les événements) postérieurs à start_date
        to_process = df[
            (df['url'].str.contains('.export.CSV.zip')) & 
            (df['datetime'] >= start_date)
        ].sort_values('datetime')

        print(f"🚀 {len(to_process)} fichiers à rattraper depuis {start_date.strftime('%Y-%m-%d %H:%M')}.")

        # 4. Boucle d'ingestion
        for idx, row in to_process.iterrows():
            url = row['url']
            date_folder = row['datetime'].strftime('%Y-%m-%d')
            
            # Téléchargement du zip CSV
            zip_r = requests.get(url)
            
            with zipfile.ZipFile(io.BytesIO(zip_r.content)) as z:
                for filename in z.namelist():
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
                    
                    s3_key = f"raw/gdelt/history/{date_folder}/{parquet_filename}"
                    
                    # Upload en Parquet
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=s3_key,
                        Body=parquet_buffer.getvalue()
                    )
                    print(f"✅ Ingested: {s3_key}")

        print(f"✨ Backfill depuis {start_date.strftime('%Y-%m-%d %H:%M')} terminé !")

    except Exception as e:
        print(f"❌ Erreur critique : {e}")

if __name__ == "__main__":
    run_backfill()