"""
Benchmark: Data Skew
====================
90% of rows share a single join key (key=0). The sort-merge join
creates one massively over-sized task while the other 9 keys finish
quickly — classic executor imbalance.

Expected waste:
  - DATA_SKEW suggestion (max task >> median task)
  - HIGH_SHUFFLE (large shuffle from sort-merge join)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("benchmark_data_skew")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    n_total = 2_000_000
    n_hot   = int(n_total * 0.90)   # 1.8M rows on key=0
    n_cold  = n_total - n_hot        # 0.2M spread across keys 1–9

    hot  = spark.range(n_hot).withColumn("key", F.lit(0)).withColumn("value", F.col("id"))
    cold = (
        spark.range(n_cold)
        .withColumn("key",   (F.col("id") % 9 + 1).cast("long"))
        .withColumn("value", F.col("id"))
    )
    df = hot.union(cold).repartition(8, "key")

    lookup = spark.createDataFrame(
        [(i, f"label_{i}") for i in range(10)],
        ["key", "label"],
    )

    result = (
        df.join(lookup, "key")
        .groupBy("key", "label")
        .agg(F.count("*").alias("cnt"), F.sum("value").alias("total"))
        .orderBy(F.desc("cnt"))
    )
    result.show(20)
    spark.stop()


if __name__ == "__main__":
    main()
