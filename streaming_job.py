import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

THRESHOLD = float(sys.argv[1]) if len(sys.argv) > 1 else 500.0

spark = SparkSession.builder.appName("aiops-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("traceid", StringType()),
    StructField("rpcid", StringType()),
    StructField("um", StringType()),
    StructField("dm", StringType()),
    StructField("rpctype", StringType()),
    StructField("rt", DoubleType()),
    StructField("timestamp", LongType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "microservices-logs")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 5000)
       .load())

parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("d"))
             .select("d.*"))

anomalies = parsed.filter(col("rt") > THRESHOLD)

# Query 1: console (testing)
q_console = (anomalies.select("traceid", "rpcid", "dm", "rt")
             .writeStream.format("console").outputMode("append").start())

# Query 2: raw archive to HDFS (Parquet)
q_raw = (parsed.writeStream.format("parquet")
         .option("path", "hdfs://hadoop-master:9000/telemetry/raw")
         .option("checkpointLocation", "hdfs://hadoop-master:9000/telemetry/checkpoints/raw")
         .trigger(processingTime="10 seconds")
         .outputMode("append").start())

# Query 3: anomalies to Kafka topic "anomalies" (a bridge writes them to HBase)
out = anomalies.select(
    col("traceid").alias("key"),
    to_json(struct("traceid", "rpcid", "um", "dm", "rt", "timestamp")).alias("value"))
q_kafka = (out.writeStream.format("kafka")
           .option("kafka.bootstrap.servers", "kafka:9092")
           .option("topic", "anomalies")
           .option("checkpointLocation", "hdfs://hadoop-master:9000/telemetry/checkpoints/anomalies")
           .outputMode("append").start())

spark.streams.awaitAnyTermination()
