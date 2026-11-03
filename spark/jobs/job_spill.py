"""
Benchmark: Memory Spill
=======================
Forces disk spill by sorting a large dataset under a tight 512 MB
executor memory budget. Padded strings inflate each row so the
sort buffer overflows to disk early.

Expected waste:
  - MEMORY_SPILL suggestion (memory_spill_bytes > 0)
  - HIGH_SHUFFLE (4-partition sort shuffle)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("benchmark_memory_spill")
        .config("spark.executor.memory", "512m")
        .config("spark.memory.fraction", "0.5")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.range(800_000)
        .withColumn("key",      (F.rand() * 100_000).cast("int"))
        .withColumn("payload",  F.expr("repeat(cast(id as string), 60)"))
        .withColumn("category", (F.col("id") % 50).cast("string"))
    )

    result = (
        df.repartition(4)
        .orderBy("key", "payload")          # forces sort-spill
        .groupBy("category")
        .agg(
            F.count("*").alias("cnt"),
            F.sum("key").alias("key_sum"),
            F.max("payload").alias("max_payload"),
        )
    )
    result.show(10)
    spark.stop()


if __name__ == "__main__":
    main()
