#!/bin/bash
# Generates synthetic, scaled-up versions of company_claims and social_signal
# for the cost/runtime benchmark. Same schema as the real tables, so the same
# Spark job and SQL run on them unchanged.
#
# The real pipeline has 10 companies, which tells you nothing about cost or speed.
# These tables are synthetic stress-test data, not real companies.
#
# Run from the repo root:  bash gcp/03_generate_benchmark_data.sh
# Optional: SCALES="1000000 10000000" bash gcp/03_generate_benchmark_data.sh
#
# Cost: generation scans almost nothing (data is created from GENERATE_ARRAY).
# 10M rows is roughly 2 GB of BigQuery storage, inside the 10 GB free tier.
# Delete the dataset when you're done benchmarking:  bq rm -r -f greenwashing_bench

set -euo pipefail
export PYTHONWARNINGS="ignore"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="europe-west2"
BUCKET="gs://${PROJECT_ID}-data"
DS="greenwashing_bench"
SCALES="${SCALES:-1000000 10000000}"

if ! bq --project_id="${PROJECT_ID}" show "${DS}" >/dev/null 2>&1; then
  bq --project_id="${PROJECT_ID}" mk --location="${REGION}" --dataset "${PROJECT_ID}:${DS}"
fi

run_sql () {
  bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --location="${REGION}" --quiet "$1"
}

label () {   # 1000000 -> 1m, 10000000 -> 10m
  echo "$(( $1 / 1000000 ))m"
}

for N in ${SCALES}; do
  L="$(label "${N}")"
  OUTER=$(( (N + 9999) / 10000 - 1 ))
  echo "=== Generating ${L} (${N} rows) ==="

  # Deterministic pseudo-randomness: same N always gives the same data,
  # so reruns of the benchmark compare like with like.
  run_sql "
  CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DS}.company_claims_${L}\` AS
  WITH ids AS (
    SELECT b * 10000 + a AS id
    FROM UNNEST(GENERATE_ARRAY(0, 9999)) AS a
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, ${OUTER})) AS b
    WHERE b * 10000 + a < ${N}
  ),
  r AS (
    SELECT id,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('nz',  CAST(id AS STRING)))), 1000) / 1000 AS u_nz,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('pct', CAST(id AS STRING)))), 1000) / 1000 AS u_pct,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('crt', CAST(id AS STRING)))), 1000) / 1000 AS u_cert,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('url', CAST(id AS STRING)))), 7)            AS n_urls
    FROM ids
  )
  SELECT
    FORMAT('C%09d', id)                                                   AS company_id,
    FORMAT('Company %09d', id)                                            AS company_name,
    ['Energy','Finance','Mining','Healthcare','Retail','Telecom','Utilities'][OFFSET(MOD(id, 7))] AS sector,
    IF(u_nz < 0.15, NULL, 2030 + 5 * CAST(FLOOR(u_nz * 5) AS INT64))     AS net_zero_target_year,
    IF(u_pct < 0.20, NULL, ROUND(10 + u_pct * 89, 1))                     AS reduction_pct_claimed,
    CASE
      WHEN u_cert < 0.30 THEN NULL
      WHEN u_cert < 0.55 THEN 'SBTi'
      WHEN u_cert < 0.80 THEN 'CDP,SBTi'
      ELSE 'CDP,SBTi,TCFD'
    END                                                                   AS certifications,
    n_urls + 1                                                            AS urls_successful,
    FORMAT('https://example.com/company/%09d/sustainability', id)        AS source_url,
    '2026-03-11T00:00:00'                                                 AS scraped_at
  FROM r"

  # About 10% of companies get no social row, so the left join and fillna matter
  run_sql "
  CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DS}.social_signal_${L}\` AS
  WITH ids AS (
    SELECT b * 10000 + a AS id
    FROM UNNEST(GENERATE_ARRAY(0, 9999)) AS a
    CROSS JOIN UNNEST(GENERATE_ARRAY(0, ${OUTER})) AS b
    WHERE b * 10000 + a < ${N}
  ),
  r AS (
    SELECT id,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('keep', CAST(id AS STRING)))), 100) AS keep,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('red',  CAST(id AS STRING)))), 120) AS reddit,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('nws',  CAST(id AS STRING)))), 60)  AS news,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('scr',  CAST(id AS STRING)))), 500) AS score,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('cmt',  CAST(id AS STRING)))), 5000) AS comments
    FROM ids
  )
  SELECT
    FORMAT('Company %09d', id) AS company_name,
    reddit                     AS reddit_post_count,
    score                      AS reddit_avg_score,
    comments                   AS reddit_total_comments,
    news                       AS news_article_count,
    reddit + news              AS total_media_signals
  FROM r
  WHERE keep >= 10"

  # Parquet copies in GCS, for the local Spark run
  for T in company_claims social_signal; do
    run_sql "
    EXPORT DATA OPTIONS (
      uri = '${BUCKET}/bench/${L}/${T}/part-*.parquet',
      format = 'PARQUET',
      overwrite = true
    ) AS SELECT * FROM \`${PROJECT_ID}.${DS}.${T}_${L}\`"
  done
done

echo "=== Tables ==="
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=pretty "
SELECT table_id, row_count, ROUND(size_bytes / POW(1024, 2), 1) AS size_mb
FROM \`${PROJECT_ID}.${DS}.__TABLES__\`
ORDER BY table_id"

echo "=== Parquet in GCS ==="
gcloud storage du -s -r "${BUCKET}/bench/" 2>/dev/null || gcloud storage du --summarize "${BUCKET}/bench/"