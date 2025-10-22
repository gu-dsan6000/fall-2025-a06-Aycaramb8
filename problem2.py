#!/usr/bin/env python3
"""
Problem 2 - Cluster Usage Analysis
"""

import os, sys, logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, min as spark_min, max as spark_max,
    countDistinct, to_timestamp, input_file_name, try_to_timestamp, lit
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session(master_url):
    """Create Spark session for cluster execution."""
    spark = (
        SparkSession.builder
        .appName("Problem2")
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


def solve_problem2(spark):
    """Perform cluster analysis"""
    S3_PATH = "s3a://xc280-assignment-spark-cluster-logs/data/application_*/*.log"
    logger.info(f"Reading logs from: {S3_PATH}")

    df = spark.read.text(S3_PATH).withColumn("file_path", input_file_name())

    # Extract cluster IDs, application IDs, and timestamps
    parsed_df = df.select(
        regexp_extract("file_path", r"application_(\d+_\d+)", 0).alias("application_id"),
        regexp_extract("file_path", r"application_(\d+)_", 1).alias("cluster_id"),
        regexp_extract("file_path", r"application_\d+_(\d+)", 1).alias("app_number"),
        regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp"),
        col("value").alias("message")
    ).withColumn("timestamp", try_to_timestamp(col("timestamp"), lit("yy/MM/dd HH:mm:ss")))

    # Only keep valid timestamp rows
    parsed_df = parsed_df.filter(col("timestamp").isNotNull())

    parsed_df.cache()

    # Time-series data for each application
    app_times = (
        parsed_df.groupBy("cluster_id", "application_id")
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        )
        .orderBy("cluster_id", "start_time")
    )

    # Aggregated cluster statistics
    cluster_summary = (
        app_times.groupBy("cluster_id")
        .agg(
            countDistinct("application_id").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app"),
        )
        .orderBy(col("num_applications").desc())
    )

    app_times.toPandas().to_csv("problem2_timeline.csv", index=False)
    cluster_summary.toPandas().to_csv("problem2_cluster_summary.csv", index=False)

    # Overall summary statistics
    total_clusters = cluster_summary.count()
    total_apps = app_times.count()
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0

    summary = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_apps:.2f}",
        "\nMost heavily used clusters:",
    ]

    # Show top
    for row in cluster_summary.take(5):
        summary.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    summary_text = "\n".join(summary)
    with open("problem2_stats.txt", "w") as f:
        f.write(summary_text)

    print(summary_text)
    logger.info(summary_text)

    print("\nSummary:")
    print(summary_text)

def generate_visuals():
    """Generate bar chart and density plot"""
    timeline = pd.read_csv("problem2_timeline.csv")
    summary = pd.read_csv("problem2_cluster_summary.csv")

    # Bar Chart
    plt.figure(figsize=(8, 5))
    ax = sns.barplot(data=summary, x="cluster_id", y="num_applications", palette="Set2")
    plt.title("Number of Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    # Value labels displayed on top of each correct id
    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='edge', padding=3, fontsize=10, color='black', weight='bold')
    plt.tight_layout()
    plt.savefig("problem2_bar_chart.png")

    # Density Plot
    timeline["duration_sec"] = (
        pd.to_datetime(timeline["end_time"]) - pd.to_datetime(timeline["start_time"])
    ).dt.total_seconds()
    top_cluster = summary.iloc[0]["cluster_id"]
    top_timeline = timeline[timeline["cluster_id"] == top_cluster]  # the largest cluster

    plt.figure(figsize=(8, 5))
    # KDE and log scale
    sns.histplot(top_timeline["duration_sec"], kde=True, log_scale=True, color="skyblue")
    plt.title(f"Job Duration Distribution (Cluster {top_cluster}, n={len(top_timeline)})")
    plt.xlabel("Duration (seconds, log scale)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig("problem2_density_plot.png")
    print("\n✅ Visualizations created successfully.")

def main():
    print("=" * 70)
    print("Problem 2")
    print("=" * 70)

    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")

        if master_private_ip:
            master_url = f"spark://{master_private_ip}:7077"
        else:
            print("❌ Error: Master URL not provided.")
            print("Usage: python problem2.py spark://MASTER_IP:7077")
            print("   or: export MASTER_PRIVATE_IP=xxx.xxx.xxx.xxx")
            return 1

    print(f"Connecting to Spark Master at: {master_url}")
    logger.info(f"Using Spark master URL: {master_url}")

    spark = create_spark_session(master_url)

    try:
        solve_problem2(spark)
        generate_visuals()
    except Exception as e:
        logger.exception(f"❌ Error during cluster analysis: {str(e)}")
        print(f"Error: {e}")
    finally:
        spark.stop()
        print("✅ Spark session stopped.")


if __name__ == "__main__":
    main()
