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
