#!/usr/bin/env python3
"""
Problem 1 - Log Level Distribution
"""

import os, sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)

def create_spark_session(master_url):
    """Create Spark session for cluster execution."""
    spark = (
        SparkSession.builder
        .appName("Problem1")
        .master(master_url)

        # Memory Configuration
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")

        # Executor Configuration
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")  # Use all available cores across cluster

        # S3 Configuration - Use S3A for AWS S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # Performance settings for cluster execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # Arrow optimization for Pandas conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    logger.info("Spark session created successfully for cluster execution")
    return spark

def solve_problem1(spark):
    """Log Level Distribution"""
    S3_PATH = "s3a://xc280-assignment-spark-cluster-logs/data/application_*/*.log"
    logger.info(f"Reading logs from: {S3_PATH}")

    df = spark.read.text(S3_PATH)

    # Use PySpark's regexp_extract for parsing log files
    parsed_df = df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'\b(INFO|WARN|ERROR|DEBUG)\b', 1).alias('log_level'),
        regexp_extract('value', r'\b(INFO|WARN|ERROR|DEBUG)\b\s+([^:]+):', 2).alias('component'),
        col('value').alias('log_entry')
    )

    # Log level counts
    log_level_counts = (
        parsed_df.filter(col("log_level") != "")
        .groupBy("log_level")
        .count()
        .orderBy(col("count").desc())
    )

    # 10 random sample log entries with their levels
    sample_df = (
        parsed_df.filter(col("log_level") != "")
        .orderBy(rand())
        .limit(10)
    )
    sample_df = sample_df.select("log_entry", "log_level")

    log_level_counts.toPandas().to_csv("problem1_counts.csv", index=False)
    sample_df.toPandas().to_csv("problem1_sample.csv", index=False)

    # Summary statistics
    total_lines = df.count()
    total_parsed = parsed_df.filter(col("log_level") != "").count()
    unique_levels = log_level_counts.count()

    summary = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_parsed:,}",
        f"Unique log levels found: {unique_levels}",
        "\nLog level distribution:"
    ]
    
    # Pecentage calculate and arrange format
    for row in log_level_counts.collect():
        pct = 100 * row["count"] / total_parsed
        summary.append(f"  {row['log_level']:<6}: {row['count']:>10,} ({pct:6.2f}%)")

    summary_text = "\n".join(summary)
    with open("problem1_summary.txt", "w") as f:
        f.write(summary_text)

    print("\nSummary:")
    print(summary_text)
    print("\n✅ Problem 1 completed successfully!")
    logger.info("Completed Problem 1 (cluster)")
    logger.info(summary_text)

def main():
    print("=" * 70)
    print("Problem 1")
    print("=" * 70)

    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")

        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided.")
            print("Usage: python problem1.py spark://MASTER_IP:7077")
            print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1

    print(f"Connecting to Spark Master at: {master_url}")
    logger.info(f"Using Spark master URL: {master_url}")

    spark = create_spark_session(master_url)

    try:
        solve_problem1(spark)
    except Exception as e:
        logger.exception(f"❌ Error during cluster analysis: {str(e)}")
        print(f"Error: {e}")
    finally:
        spark.stop()
        print("✅ Spark session stopped.")

if __name__ == "__main__":
    main()