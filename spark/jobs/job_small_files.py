"""
Benchmark: Small Files / Excessive Partitions
=============================================
Splits a modest dataset into 300 tiny partitions, generating 300
shuffle tasks each reading < 1 MB. Simulates reading from a table
with many small files — massive task scheduling overhead vs actual work.

Expected waste:
  - SMALL_FILES suggestion (avg input per task < 1 MB)
  - HIGH_SHUFFLE (300-way shuffle)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("benchmark_small_files")
        .config("spark.sql.shuffle.partitions", "300")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.range(600_000)
        .withColumn("bucket", (F.col("id") % 300).cast("int"))
        .withColumn("value",   F.rand() * 1000)
        .withColumn("label",   F.expr("concat('item_', cast(id % 1000 as string))"))
    )

    # Repartition into 300 tiny partitions — the anti-pattern
    df_partitioned = df.repartition(300, "bucket")

    result = (
        df_partitioned
        .groupBy("bucket")
        .agg(
            F.count("*").alias("cnt"),
            F.avg("value").alias("avg_val"),
            F.stddev("value").alias("std_val"),
            F.countDistinct("label").alias("distinct_labels"),
        )
        .orderBy("bucket")
    )
    result.show(20)

    total = df_partitioned.count()
    print(f"Total records: {total}")
    spark.stop()


if __name__ == "__main__":
    main()
