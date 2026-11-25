#!/bin/bash
# Uploads the pipeline's Parquet outputs to Cloud Storage and loads them into BigQuery.
# Run from the repo root:  bash gcp/01_load_bigquery.sh
# Safe to re-run: tables are replaced, bucket/datasets are only created if missing.

set -euo pipefail
export PYTHONWARNINGS="ignore"   # hides the harmless urllib3 warning from the bq tool

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="europe-west2"
BUCKET="gs://${PROJECT_ID}-data"

echo "Project: ${PROJECT_ID}  Region: ${REGION}  Bucket: ${BUCKET}"

# 1. Bucket (same region as BigQuery so loads stay in-region)
if ! gcloud storage buckets describe "${BUCKET}" >/dev/null 2>&1; then
  gcloud storage buckets create "${BUCKET}" --location="${REGION}" --uniform-bucket-level-access
fi

# 2. Upload raw + processed Parquet
gcloud storage cp data/raw/*.parquet       "${BUCKET}/raw/"
gcloud storage cp data/processed/*.parquet "${BUCKET}/processed/"

# 3. Datasets
for DS in greenwashing_raw greenwashing_processed; do
  if ! bq --project_id="${PROJECT_ID}" show "${DS}" >/dev/null 2>&1; then
    bq --project_id="${PROJECT_ID}" mk --location="${REGION}" --dataset "${PROJECT_ID}:${DS}"
  fi
done

# 4. One table per Parquet file (schema comes from the Parquet file itself)
load_layer () {
  local layer="$1" dataset="$2"
  for f in data/${layer}/*.parquet; do
    local table
    table="$(basename "${f}" .parquet)"
    echo "Loading ${dataset}.${table}"
    bq --project_id="${PROJECT_ID}" load --replace --source_format=PARQUET \
      --parquet_enable_list_inference \
      "${dataset}.${table}" "${BUCKET}/${layer}/${table}.parquet"
  done
}

load_layer raw       greenwashing_raw
load_layer processed greenwashing_processed

# 5. Row counts, to check against the local files
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=pretty "
SELECT dataset_id AS dataset, table_id AS table_name, row_count
FROM \`${PROJECT_ID}.greenwashing_raw.__TABLES__\`
UNION ALL
SELECT dataset_id, table_id, row_count
FROM \`${PROJECT_ID}.greenwashing_processed.__TABLES__\`
ORDER BY dataset, table_name"