"""
Benchmark: the same greenwashing risk scoring on three engines, at 1M and 10M companies.

  local_spark  - PySpark on this machine, reading Parquet downloaded from GCS
  dataproc     - Dataproc Serverless, reading/writing BigQuery (gcp/spark_risk_scoring.py)
  bigquery_sql - one CREATE TABLE AS SELECT in BigQuery, same scoring rules in SQL

For each run it records wall-clock time and what GCP actually billed for
(DCU-seconds for Dataproc, bytes billed and slot-ms for BigQuery), then checks
that all three engines produce identical risk-category counts.

Run from the repo root (needs 03_generate_benchmark_data.sh first):
    python gcp/04_benchmark.py
    python gcp/04_benchmark.py --scales 1m          # quicker first pass
    python gcp/04_benchmark.py --skip local_spark   # if PySpark/Java aren't set up

Output: gcp/benchmark_results.csv (commit this) and a printed summary.

Cost estimates use list prices passed in below. They are us-central1 list rates;
europe-west2 is slightly higher, so check the pricing pages and override with
--dcu-rate / --bq-tib-rate if you want exact London figures. BigQuery's first
1 TiB of queries each month is free, so the BigQuery runs here actually cost 0.
"""

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

REGION = "europe-west2"
DS = "greenwashing_bench"
ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "gcp" / "logs"
ENV = {
    **os.environ,
    "PYTHONWARNINGS": "ignore",
    # gcloud's sliced (parallel) downloads can report a bogus all-zero checksum
    # under some local Python setups; download each file in one piece instead
    "CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD": "0",
}


def sh(cmd, capture=True, check=True):
    """Run a command, return stdout. Raises with the stderr shown if it fails."""
    p = subprocess.run(cmd, capture_output=capture, text=True, env=ENV)
    if check and p.returncode != 0:
        print(p.stdout or "")
        print(p.stderr or "", file=sys.stderr)
        raise SystemExit(f"Command failed: {' '.join(cmd)}")
    return p.stdout if capture else ""


def project_id():
    return os.environ.get("PROJECT_ID") or sh(["gcloud", "config", "get-value", "project"]).strip()


def bq_category_counts(project, table):
    out = sh(["bq", f"--project_id={project}", "query", "--use_legacy_sql=false",
              f"--location={REGION}", "--format=json", "--quiet",
              f"SELECT risk_category, COUNT(*) AS n FROM `{project}.{DS}.{table}` "
              f"GROUP BY risk_category ORDER BY risk_category"])
    return {r["risk_category"]: int(r["n"]) for r in json.loads(out)}


# ---------------------------------------------------------------- BigQuery SQL

SCORING_SQL = """
CREATE OR REPLACE TABLE `{p}.{ds}.scored_bq_{L}` AS
WITH merged AS (
  SELECT
    c.*,
    COALESCE(s.reddit_post_count, 0)     AS reddit_post_count,
    COALESCE(s.reddit_avg_score, 0)      AS reddit_avg_score,
    COALESCE(s.reddit_total_comments, 0) AS reddit_total_comments,
    COALESCE(s.news_article_count, 0)    AS news_article_count,
    COALESCE(s.total_media_signals, 0)   AS total_media_signals
  FROM `{p}.{ds}.company_claims_{L}` c
  LEFT JOIN `{p}.{ds}.social_signal_{L}` s USING (company_name)
),
flags AS (
  SELECT *,
    IF(net_zero_target_year IS NOT NULL, 1, 0)                            AS has_net_zero_target,
    IF(reduction_pct_claimed > 30, 1, 0)                                  AS high_reduction_claim,
    IF(certifications IS NULL, 1, 0)                                      AS no_certification,
    IF(total_media_signals > 50, 1, 0)                                    AS high_media_attention,
    IF(reduction_pct_claimed > 30 AND certifications IS NULL, 1, 0)       AS claim_without_cert
  FROM merged
),
scored AS (
  SELECT *, claim_without_cert * 2 + no_certification + high_media_attention AS greenwashing_risk_score
  FROM flags
)
SELECT *,
  CASE WHEN greenwashing_risk_score >= 3 THEN 'HIGH'
       WHEN greenwashing_risk_score = 2  THEN 'MEDIUM'
       WHEN greenwashing_risk_score = 1  THEN 'LOW'
       ELSE 'MINIMAL' END                                                 AS risk_category,
  CAST(CURRENT_TIMESTAMP() AS STRING)                                     AS processed_at
FROM scored
"""


def run_bigquery_sql(project, L, rates):
    job_id = f"bench_bq_{L}_{uuid.uuid4().hex[:8]}"
    t0 = time.time()
    sh(["bq", f"--project_id={project}", "query", "--use_legacy_sql=false",
        f"--location={REGION}", f"--job_id={job_id}", "--quiet",
        SCORING_SQL.format(p=project, ds=DS, L=L)])
    wall = time.time() - t0

    job = json.loads(sh(["bq", f"--project_id={project}", "--format=json", "show",
                         f"--location={REGION}", "-j", job_id]))
    st = job["statistics"]
    bytes_billed = int(st.get("query", {}).get("totalBytesBilled", 0))
    slot_ms = int(st.get("query", {}).get("totalSlotMs", st.get("totalSlotMs", 0)))
    engine_s = (int(st["endTime"]) - int(st["startTime"])) / 1000

    return {
        "wall_s": round(wall, 1),
        "engine_s": round(engine_s, 1),
        "bytes_billed_gb": round(bytes_billed / 1024**3, 3),
        "slot_s": round(slot_ms / 1000, 1),
        "dcu_s": "",
        "est_cost_usd": round(bytes_billed / 1024**4 * rates["bq_tib"], 4),
        "categories": bq_category_counts(project, f"scored_bq_{L}"),
        "ref": job_id,
    }


# ------------------------------------------------------------------- Dataproc

def run_dataproc(project, L, rates):
    bucket = f"gs://{project}-data"
    sh(["gcloud", "storage", "cp", str(ROOT / "gcp" / "spark_risk_scoring.py"),
        f"{bucket}/jobs/spark_risk_scoring.py"])
    batch = f"bench-{L}-{datetime.now():%Y%m%d-%H%M%S}"

    t0 = time.time()
    p = subprocess.run(["gcloud", "dataproc", "batches", "submit", "pyspark",
              f"{bucket}/jobs/spark_risk_scoring.py",
              f"--project={project}", f"--region={REGION}", f"--batch={batch}",
              "--version=2.2", f"--deps-bucket={bucket}", "--ttl=30m",
              "--properties=spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4",
              "--",
              "--source", "bigquery",
              "--claims", f"{project}.{DS}.company_claims_{L}",
              "--social", f"{project}.{DS}.social_signal_{L}",
              "--output", f"{project}.{DS}.scored_dataproc_{L}"],
                       capture_output=True, text=True, env=ENV)
    wall = time.time() - t0
    # gcloud streams the job's own print output on stderr, so keep both
    log = p.stdout + "\n" + p.stderr
    (LOG_DIR / f"{batch}.log").write_text(log)
    if p.returncode != 0:
        print(p.stderr[-3000:], file=sys.stderr)
        raise SystemExit(f"Dataproc batch {batch} failed; full log in gcp/logs/{batch}.log")

    # Billed usage is filled in shortly after the batch ends, so poll for it
    usage = {}
    for _ in range(18):
        desc = json.loads(sh(["gcloud", "dataproc", "batches", "describe", batch,
                              f"--region={REGION}", f"--project={project}", "--format=json"]))
        usage = desc.get("runtimeInfo", {}).get("approximateUsage", {})
        if int(usage.get("milliDcuSeconds", 0)) > 0:
            break
        time.sleep(10)
    else:
        print(f"  note: no DCU usage reported yet for {batch}; cost shown is the 1-minute minimum")
    dcu_s = int(usage.get("milliDcuSeconds", 0)) / 1000
    shuffle_gb_s = float(usage.get("shuffleStorageGbSeconds", 0))

    job_total = re.search(r"\[timing\] total_s=([\d.]+)", log)
    return {
        "wall_s": round(wall, 1),
        "engine_s": float(job_total.group(1)) if job_total else "",
        "bytes_billed_gb": "",
        "slot_s": "",
        "dcu_s": round(dcu_s, 1),
        # 1-minute minimum per batch; shuffle storage is negligible at this size
        "est_cost_usd": round(max(dcu_s, 60 * 12) / 3600 * rates["dcu"]
                              + shuffle_gb_s / 3600 * rates["shuffle_gb_h"], 4),
        "categories": bq_category_counts(project, f"scored_dataproc_{L}"),
        "ref": batch,
    }


# ---------------------------------------------------------------- local Spark

def local_spark_ready():
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return "PySpark not installed (pip install pyspark)"
    if shutil.which("java") is None:
        return "Java not found (brew install openjdk@17)"
    return None


def run_local_spark(project, L, rates):
    local_dir = ROOT / "data" / "bench" / L
    done_marker = local_dir / ".download_complete"
    if not done_marker.exists():
        # start clean so a half-finished earlier download is never used
        shutil.rmtree(local_dir, ignore_errors=True)
        print(f"  downloading {L} Parquet from GCS ...")
        local_dir.parent.mkdir(parents=True, exist_ok=True)
        sh(["gcloud", "storage", "cp", "-r", f"gs://{project}-data/bench/{L}", str(local_dir.parent)])
        done_marker.touch()

    out_dir = "noop" if rates.get("local_no_write") else str(ROOT / "data" / "bench_out" / f"local_{L}")
    t0 = time.time()
    p = subprocess.run([sys.executable, str(ROOT / "gcp" / "spark_risk_scoring.py"),
                        "--source", "parquet",
                        "--claims", str(local_dir / "company_claims"),
                        "--social", str(local_dir / "social_signal"),
                        "--output", out_dir],
                       capture_output=True, text=True, env=ENV)
    wall = time.time() - t0
    (LOG_DIR / f"local_{L}.log").write_text(p.stdout + "\n" + p.stderr)
    if p.returncode != 0:
        print(p.stderr[-3000:], file=sys.stderr)
        raise SystemExit(f"Local Spark failed for {L}; full log in gcp/logs/local_{L}.log")

    result = re.search(r"\[result\] (\{.*\})", p.stdout)
    job_total = re.search(r"\[timing\] total_s=([\d.]+)", p.stdout)
    return {
        "wall_s": round(wall, 1),
        "engine_s": float(job_total.group(1)) if job_total else "",
        "bytes_billed_gb": "",
        "slot_s": "",
        "dcu_s": "",
        "est_cost_usd": 0.0,
        "categories": json.loads(result.group(1)) if result else {},
        "ref": "local (no write, compute only)" if out_dir == "noop" else "local",
    }


# ----------------------------------------------------------------------- main

ENGINES = {"local_spark": run_local_spark, "dataproc": run_dataproc, "bigquery_sql": run_bigquery_sql}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", nargs="+", default=["1m", "10m"])
    ap.add_argument("--skip", nargs="*", default=[], choices=list(ENGINES))
    ap.add_argument("--dcu-rate", type=float, default=0.06, help="USD per DCU-hour")
    ap.add_argument("--shuffle-rate", type=float, default=0.000054795, help="USD per GiB-hour")
    ap.add_argument("--bq-tib-rate", type=float, default=6.25, help="USD per TiB billed")
    ap.add_argument("--local-no-write", action="store_true",
                    help="local Spark skips writing output (use if disk space is short)")
    args = ap.parse_args()

    rates = {"dcu": args.dcu_rate, "shuffle_gb_h": args.shuffle_rate, "bq_tib": args.bq_tib_rate,
             "local_no_write": args.local_no_write}
    project = project_id()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    engines = [e for e in ENGINES if e not in args.skip]
    if "local_spark" in engines:
        problem = local_spark_ready()
        if problem:
            print(f"Skipping local_spark: {problem}")
            engines.remove("local_spark")

    cols = ["scale", "engine", "status", "wall_s", "engine_s", "dcu_s", "slot_s", "bytes_billed_gb",
            "est_cost_usd", "categories_match", "categories", "ref"]
    out = ROOT / "gcp" / "benchmark_results.csv"

    def save(rows):
        # every engine that succeeded at a given scale must agree exactly
        for L in args.scales:
            ok = [r for r in rows if r["scale"] == L and r["status"] == "ok"]
            match = len({json.dumps(r["categories"], sort_keys=True) for r in ok}) == 1
            for r in ok:
                r["categories_match"] = match if len(ok) > 1 else ""
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({**r, "categories": json.dumps(r.get("categories", {}), sort_keys=True)})

    rows = []
    for L in args.scales:
        for engine in engines:
            print(f"[{L}] {engine} ...", flush=True)
            try:
                r = ENGINES[engine](project, L, rates)
                r["status"] = "ok"
                print(f"  wall {r['wall_s']}s, est ${r['est_cost_usd']}, categories {r['categories']}")
            except (Exception, SystemExit) as e:
                # record the failure and keep going, so one engine can't wipe the others' results
                r = {"status": f"failed: {str(e)[:120]}", "categories": {}}
                print(f"  FAILED: {e}")
            r.update({"scale": L, "engine": engine})
            rows.append(r)
            save(rows)  # saved after every run

    show = ["scale", "engine", "status", "wall_s", "engine_s", "dcu_s", "slot_s", "est_cost_usd", "categories_match"]
    print("\n" + " | ".join(f"{c:>14}" for c in show))
    for r in rows:
        print(" | ".join(f"{str(r.get(c, ''))[:14]:>14}" for c in show))
    print(f"\nSaved {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()