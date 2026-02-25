#!/bin/bash
# ============================================================
# localstack_init.sh
# Script exécuté au démarrage pour créer les buckets S3
# dans LocalStack (le faux AWS en local).
#
# Structure Data Lake V2.2 :
#   s3://datalake/
#     ├── raw/          ← Données brutes (JSON ou CSV)
#     ├── formatted/    ← Données nettoyées (Parquet)
#     └── combined/     ← Résultat final (Parquet) 
# ============================================================

echo "Attente que LocalStack soit prêt..."
until curl -sf http://localhost:4566/_localstack/health | grep -q '"s3": "available"'; do
  sleep 2
done
echo "LocalStack est prêt !"

# Créer le bucket principal du Data Lake
awslocal s3 mb s3://datalake

# Créer la hiérarchie de dossiers (conventions Data Lake V2.2)
# ── raw/ : données brutes telles que récupérées des APIs
awslocal s3api put-object --bucket datalake --key raw/yahoofinance/
awslocal s3api put-object --bucket datalake --key raw/gdelt/

# ── formatted/ : données nettoyées, en Parquet, dates UTC
awslocal s3api put-object --bucket datalake --key formatted/yahoofinance/
awslocal s3api put-object --bucket datalake --key formatted/gdelt/

# ── combined/ : résultat final (jointure GDELT + FRED) 
# A MODIFIER SI AJOUT DE COMBINAISON 
awslocal s3api put-object --bucket datalake --key combined/stress_index/

echo "Bucket 'datalake' créé avec la structure suivante :"
awslocal s3 ls s3://datalake/ --recursive
echo "Initialisation terminée !"
