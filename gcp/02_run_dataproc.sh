#!/bin/bash
# Runs gcp/spark_risk_scoring.py on Dataproc Serverless, reading from and writing to BigQuery.
# Run from the repo root:  bash gcp/02_run_dataproc.sh
# Needs 01_load_bigquery.sh to have run first.
#
# Cost: a few pence per run. Serverless batches shut down when the job ends,
# and --ttl kills the batch after 30 min no matter what.

set -euo pipefail
export PYTHONWARNINGS="ignore"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="europe-west2"
BUCKET="gs://${PROJECT_ID}-data"
OUT_DATASET="greenwashing_spark"

echo "Project: ${PROJECT_ID}  Region: ${REGION}"

# --- One-time setup (skipped quickly if already done) ------------------------

# Compute API is needed for the network Dataproc runs in
gcloud services enable compute.googleapis.com dataproc.googleapis.com bigquerystorage.googleapis.com

# Dataproc Serverless workers have no public IP, so the subnet needs
# Private Google Access to reach BigQuery and Cloud Storage
gcloud compute networks subnets update default \
  --region="${REGION}" --enable-private-ip-google-access --quiet

# The job runs as the default compute service account; give it what it needs
PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
for ROLE in roles/dataproc.worker roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/bigquery.readSessionUser roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA}" --role="${ROLE}" --condition=None --quiet >/dev/null
done

# Output dataset
if ! bq --project_id="${PROJECT_ID}" show "${OUT_DATASET}" >/dev/null 2>&1; then
  bq --project_id="${PROJECT_ID}" mk --location="${REGION}" --dataset "${PROJECT_ID}:${OUT_DATASET}"
fi

# --- Run the job -------------------------------------------------------------

gcloud storage cp gcp/spark_risk_scoring.py "${BUCKET}/jobs/spark_risk_scoring.py"

BATCH_ID="risk-scoring-$(date +%Y%m%d-%H%M%S)"
START=$(date +%s)

gcloud dataproc batches submit pyspark "${BUCKET}/jobs/spark_risk_scoring.py" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --batch="${BATCH_ID}" \
  --version=2.2 \
  --deps-bucket="${BUCKET}" \
  --ttl=30m \
  --properties="spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4" \
  -- \
  --source bigquery \
  --claims "${PROJECT_ID}.greenwashing_raw.company_claims" \
  --social "${PROJECT_ID}.greenwashing_raw.social_signal" \
  --output "${PROJECT_ID}.${OUT_DATASET}.gw_scored"

END=$(date +%s)
echo "[timing] submit_to_finish_s=$((END - START))  (includes Serverless start-up)"

# --- Check: Dataproc scores must match the team's original output -----------

bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=pretty "
SELECT
  COUNT(*)                                                    AS companies,
  COUNTIF(s.greenwashing_risk_score = o.greenwashing_risk_score
          AND s.risk_category = o.risk_category)              AS scores_matching_original
FROM \`${PROJECT_ID}.${OUT_DATASET}.gw_scored\` s
JOIN \`${PROJECT_ID}.greenwashing_processed.gw_final\` o USING (company_name)"