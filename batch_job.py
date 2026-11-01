import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

THRESHOLD = 500.0
BASE = "hdfs://hadoop-master:9000/telemetry"

spark = SparkSession.builder.appName("aiops-batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(BASE + "/raw")
print("raw rows in archive:", df.count())

# exclude the unknown-service placeholder for per-service statistics
clean = df.filter((F.col("dm") != "(?)") & (F.col("rt") >= 0))

stats = (clean.groupBy("dm").agg(
    F.count("*").alias("calls"),
    F.round(F.avg("rt"), 2).alias("avg_rt"),
    F.expr("percentile_approx(rt, 0.95)").alias("p95_rt"),
    F.max("rt").alias("max_rt"),
    F.sum(F.when(F.col("rt") > THRESHOLD, 1).otherwise(0)).alias("slow_calls"),
))

stats.cache()
print("services:", stats.count())

# 1) HDFS output
(stats.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(BASE + "/metrics"))

# 2) HBase output (result is small: one row per service)
rows = stats.collect()
conn = happybase.Connection("localhost", 9090)
table = conn.table("service_metrics")
with table.batch(batch_size=500) as b:
    for r in rows:
        b.put(r["dm"].encode(), {
            b"stats:calls": str(r["calls"]).encode(),
            b"stats:avg_rt": str(r["avg_rt"]).encode(),
            b"stats:p95_rt": str(r["p95_rt"]).encode(),
            b"stats:max_rt": str(r["max_rt"]).encode(),
            b"stats:slow_calls": str(r["slow_calls"]).encode(),
        })
print("written to HBase:", len(rows))

print("Top 10 services by slow calls:")
stats.orderBy(F.desc("slow_calls")).show(10, False)
