"""
Greenwashing risk scoring as a standalone PySpark job.

Same logic as the Spark section of main.ipynb (join claims + social signal,
derive flags, score 0-4, categorise), packaged so it runs unchanged:

  - locally, reading/writing Parquet:
      python gcp/spark_risk_scoring.py --source parquet \
          --claims data/raw/company_claims.parquet \
          --social data/raw/social_signal.parquet \
          --output data/processed/gw_spark_local.parquet

  - on Dataproc Serverless, reading/writing BigQuery:
      submitted by gcp/02_run_dataproc.sh

Prints wall-clock time per stage so the local and cloud runs can be compared.
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["parquet", "bigquery"], required=True)
    p.add_argument("--claims", required=True, help="Parquet path or project.dataset.table")
    p.add_argument("--social", required=True, help="Parquet path or project.dataset.table")
    p.add_argument("--output", required=True, help="Parquet path or project.dataset.table")
    return p.parse_args()


def read(spark, source, ref):
    if source == "parquet":
        return spark.read.parquet(ref)
    return spark.read.format("bigquery").option("table", ref).load()


def write(df, source, ref):
    if ref == "noop":
        # runs the full plan but discards the output: compute-only benchmark, no disk needed
        df.write.format("noop").mode("overwrite").save()
    elif source == "parquet":
        df.write.mode("overwrite").parquet(ref)
    else:
        # direct write uses the BigQuery Storage Write API, no temp GCS bucket needed
        (df.write.format("bigquery")
           .option("writeMethod", "direct")
           .mode("overwrite")
           .save(ref))


def score(claims, social):
    media_cols = ["reddit_post_count", "reddit_avg_score", "reddit_total_comments",
                  "news_article_count", "total_media_signals"]

    merged = claims.join(social, on="company_name", how="left").fillna(0, subset=media_cols)

    return (
        merged
        .withColumn("has_net_zero_target",
                    when(col("net_zero_target_year").isNotNull(), 1).otherwise(0))
        .withColumn("high_reduction_claim",
                    when(col("reduction_pct_claimed") > 30, 1).otherwise(0))
        .withColumn("no_certification",
                    when(col("certifications").isNull(), 1).otherwise(0))
        .withColumn("high_media_attention",
                    when(col("total_media_signals") > 50, 1).otherwise(0))
        .withColumn("claim_without_cert",
                    when((col("reduction_pct_claimed") > 30) & col("certifications").isNull(), 1)
                    .otherwise(0))
        # +2 ambitious claim with no certification, +1 no certification, +1 high media attention
        .withColumn("greenwashing_risk_score",
                    col("claim_without_cert") * 2 + col("no_certification") + col("high_media_attention"))
        .withColumn("risk_category",
                    when(col("greenwashing_risk_score") >= 3, "HIGH")
                    .when(col("greenwashing_risk_score") == 2, "MEDIUM")
                    .when(col("greenwashing_risk_score") == 1, "LOW")
                    .otherwise("MINIMAL"))
        .withColumn("processed_at", lit(datetime.now(timezone.utc).isoformat()))
    )


def main():
    args = parse_args()
    t0 = time.time()

    builder = SparkSession.builder.appName("greenwashing-risk-scoring")
    if args.source == "parquet":
        # local runs only: give the driver enough memory for the 10M benchmark
        builder = builder.config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    t_start = time.time()

    claims = read(spark, args.source, args.claims)
    social = read(spark, args.source, args.social)
    result = score(claims, social).cache()
    n = result.count()  # forces the read + join + scoring to actually run
    t_compute = time.time()

    write(result, args.source, args.output)
    t_write = time.time()

    print(f"[timing] source={args.source} rows_out={n}")
    print(f"[timing] spark_startup_s={t_start - t0:.2f}")
    print(f"[timing] read_join_score_s={t_compute - t_start:.2f}")
    print(f"[timing] write_s={t_write - t_compute:.2f}")
    print(f"[timing] total_s={t_write - t0:.2f}")

    counts = {r["risk_category"]: r["count"] for r in result.groupBy("risk_category").count().collect()}
    print("[result] " + json.dumps(dict(sorted(counts.items()))))
    spark.stop()


if __name__ == "__main__":
    main()