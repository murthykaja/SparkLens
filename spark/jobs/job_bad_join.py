"""
Benchmark: Bad Join Strategy (missing broadcast)
=================================================
Two tiny dimension tables (50 products, 10 000 users) are joined with
a 2 M-row fact table using sort-merge joins because
autoBroadcastJoinThreshold is set to -1. Both dimensions easily fit
in broadcast memory — the right fix is a single config line.

Expected waste:
  - HIGH_SHUFFLE (two sort-merge shuffles)
  - Suggestion: enable broadcast join / raise threshold
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("benchmark_bad_join")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # disable auto-broadcast
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    facts = (
        spark.range(2_000_000)
        .withColumn("product_id", (F.col("id") % 50).cast("int"))
        .withColumn("user_id",    (F.col("id") % 10_000).cast("int"))
        .withColumn("amount",      F.rand() * 500)
    )

    products = spark.createDataFrame(
        [(i, f"Product_{i}", f"cat_{i % 5}") for i in range(50)],
        ["product_id", "product_name", "category"],
    )

    users = spark.createDataFrame(
        [(i, f"user_{i}", i % 3) for i in range(10_000)],
        ["user_id", "username", "tier"],
    )

    # Two back-to-back sort-merge joins — expensive and unnecessary
    result = (
        facts
        .join(products, "product_id")
        .join(users, "user_id")
        .groupBy("category", "tier")
        .agg(
            F.count("*").alias("transactions"),
            F.sum("amount").alias("revenue"),
            F.avg("amount").alias("avg_amount"),
        )
        .orderBy(F.desc("revenue"))
    )
    result.show(20)
    spark.stop()


if __name__ == "__main__":
    main()
