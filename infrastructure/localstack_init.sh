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

# Créer le bucket principal uniquement s'il n'existe pas déjà
# (idempotent : ne détruit pas les données existantes au redémarrage)
if awslocal s3 ls s3://datalake/ > /dev/null 2>&1; then
    echo "Bucket 'datalake' existe déjà — données préservées."
else
    echo "Création du bucket 'datalake'..."
    awslocal s3 mb s3://datalake

    # ── raw/ : données brutes telles que récupérées des APIs
    awslocal s3api put-object --bucket datalake --key raw/yahoofinance/
    awslocal s3api put-object --bucket datalake --key raw/yahoofinance/history/
    awslocal s3api put-object --bucket datalake --key raw/yahoofinance/daily/
    awslocal s3api put-object --bucket datalake --key raw/gdelt/
    awslocal s3api put-object --bucket datalake --key raw/gdelt/history/
    awslocal s3api put-object --bucket datalake --key raw/gdelt/daily/

    # ── formatted/ : données nettoyées, en Parquet, dates UTC
    awslocal s3api put-object --bucket datalake --key formatted/yahoofinance/
    awslocal s3api put-object --bucket datalake --key formatted/gdelt/

    # ── combined/ : résultat final
    awslocal s3api put-object --bucket datalake --key combined/stress_index/

    echo "Bucket 'datalake' créé avec la structure suivante :"
fi

awslocal s3 ls s3://datalake/ --recursive
echo "Initialisation terminée !"
