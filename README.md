# AIOps Real-Time Microservice Root-Cause Platform

## Complete Source-Code and Implementation Guide

This document explains the six Python files used in the project, the
libraries and technologies selected, how the files communicate, and how
to explain the implementation during an oral defense.

> **Scope:** The code listings below reproduce the six source files
> supplied for this guide. Explanations are based on those
> implementations and the project README. Where an implementation detail
> has a limitation, it is identified rather than silently changed.

------------------------------------------------------------------------

# 1. Project overview

The project is a small AIOps pipeline that replays microservice call
records from a CSV file as a stream. Spark processes the incoming
records, identifies slow calls using a response-time threshold, archives
valid records in HDFS, and sends anomalies to HBase. A separate batch
job calculates historical statistics per service. FastAPI exposes the
stored data, and a Streamlit dashboard visualizes live anomalies and
batch metrics.

## 1.1 The problem

A single user request can pass through multiple microservices. If the
request becomes slow, engineers need to identify which downstream
service may be responsible. This is a root-cause analysis (RCA) problem.

The dataset does not provide an error-status label. The project
therefore defines an anomaly using response time:

-   Normal rule: a call is anomalous when `rt > 500 ms`.
-   Demo rule: the streaming job can be launched with `200` ms to
    produce more visible anomalies during a demonstration.

The demo threshold is a demonstration setting; it does not replace the
stated 500 ms rule.

## 1.2 Architecture

``` text
MS_CallGraph_sample.csv
        |
        v
 producer.py
        |
        v
 Kafka topic: microservices-logs
        |
        v
 streaming_job.py (Spark Structured Streaming)
        |
        +------------------------------+
        |                              |
        v                              v
 HDFS /telemetry/raw             Kafka topic: anomalies
 (Parquet archive)                     |
        |                              v
        v                       kafka_to_hbase.py
 batch_job.py                          |
        |                              v
        v                       HBase: anomalies
 HDFS /telemetry/metrics               |
        |                              |
        v                              |
 HBase: service_metrics                |
        |                              |
        +---------------+--------------+
                        |
                        v
                  FastAPI (api.py)
                        |
                        v
             Streamlit (dashboard.py)
```

There are two processing paths:

### Speed layer

The speed layer processes incoming records while the replay is running:

1.  `producer.py` publishes records to Kafka.
2.  `streaming_job.py` reads the Kafka topic.
3.  Spark parses the JSON and filters slow calls.
4.  Anomalies are published to a second Kafka topic.
5.  `kafka_to_hbase.py` consumes that topic and writes anomaly records
    to HBase.

This path answers: **Which calls are slow in the stream?**

### Batch layer

The batch layer analyzes the archived history:

1.  Spark reads the Parquet archive from HDFS.
2.  `batch_job.py` groups records by downstream service (`dm`).
3.  It calculates call counts, average latency, approximate 95th
    percentile, maximum latency, and slow-call counts.
4.  It writes the metrics to HDFS and HBase.

This path answers: **What is the usual latency behavior of each service
over the archived data?**

The dashboard combines the two views.

------------------------------------------------------------------------

# 2. Technologies and libraries: what they do and why they are used

  -----------------------------------------------------------------------
  Technology / library    Role in this project    Why it is used
  ----------------------- ----------------------- -----------------------
  Python                  Implementation language Used for ingestion,
                                                  bridge, batch
                                                  orchestration, API, and
                                                  dashboard code

  Docker                  Runs the                Provides a reproducible
                          distributed-system      environment for Hadoop,
                          components in           Spark, HBase, Kafka,
                          containers              and Zookeeper

  Kafka                   Message broker          Decouples the CSV
                                                  producer from Spark and
                                                  buffers messages for
                                                  consumers

  Zookeeper               Kafka coordination      Used by the selected
                          service in this setup   Kafka deployment to
                                                  coordinate the broker

  Spark Structured        Stream processing       Reads Kafka records,
  Streaming               engine                  parses them, filters
                                                  anomalies, and writes
                                                  outputs

  Spark SQL / DataFrames  Structured data         Provides schema-aware
                          processing              transformations and
                                                  aggregations

  HDFS                    Distributed file        Stores the raw Parquet
                          storage                 archive and batch
                                                  metrics

  Parquet                 Columnar file format    Preserves schema and
                                                  supports efficient
                                                  analytical reads

  HBase                   NoSQL wide-column       Stores anomalies and
                          database                service metrics for
                                                  low-latency access

  HappyBase               Python HBase client     Connects Python
                                                  processes to HBase
                                                  through its Thrift
                                                  server

  FastAPI                 HTTP API framework      Exposes health,
                                                  anomaly, and
                                                  service-statistics
                                                  endpoints

  Uvicorn                 ASGI server             Runs the FastAPI
                                                  application

  Streamlit               Dashboard framework     Builds the interactive
                                                  dashboard in Python

  Requests                HTTP client             Calls the FastAPI
                                                  endpoints from the
                                                  dashboard

  Pandas                  Data manipulation       Converts API results
                          library                 into DataFrames for
                                                  grouping and plotting

  Altair                  Visualization library   Creates bar charts,
                                                  scatter plots, and
                                                  latency comparisons

  argparse                Python standard library Reads producer
                                                  command-line arguments

  csv                     Python standard library Reads the source CSV
                                                  file

  json                    Python standard library Serializes producer
                                                  messages and
                                                  deserializes Kafka
                                                  messages

  time                    Python standard library Controls the
                                                  approximate message
                                                  publishing rate

  happybase               Python HBase client     Used by the batch job,
                                                  bridge, and API to
                                                  read/write HBase
  -----------------------------------------------------------------------

## 2.1 Why Kafka instead of sending records directly to HDFS?

Kafka separates ingestion from processing. The producer can publish
records without waiting for Spark to process each one. Kafka retains
messages for consumers, supports independent consumers, and allows Spark
to resume from its offsets when checkpoints are configured.

## 2.2 Why Spark instead of MapReduce?

Spark provides a DataFrame API for both streaming and batch processing.
It is suitable for this project because the same processing ecosystem
can parse the stream and later aggregate the archive. Traditional
MapReduce is more disk-oriented and does not provide the same Structured
Streaming workflow.

## 2.3 Why HDFS and HBase?

They serve different access patterns:

-   HDFS is used for large files and sequential analytical reads/writes.
-   HBase is used for keyed records and interactive lookups by the API.

HDFS is not designed as a low-latency random-row database. HBase
complements it.

## 2.4 Why Parquet?

Parquet is a columnar, compressed, schema-aware format. Batch analytics
often read only selected columns and aggregate large datasets, so a
columnar format is useful.

------------------------------------------------------------------------

# 3. File 1 --- `producer.py`

## 3.1 Purpose

`producer.py` reads the sample CSV file, validates each record, creates
a Python dictionary for valid records, and publishes that dictionary to
the Kafka topic `microservices-logs`.

It also controls the approximate publishing rate and counts skipped
records.

## 3.2 Complete source code

``` python
import argparse
import csv
import json
import time
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--file", required=True)
parser.add_argument("--rate", type=int, default=800)
parser.add_argument("--topic", default="microservices-logs")
parser.add_argument("--bootstrap", default="kafka:9092")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20,
)

sent = 0
skipped = 0
window_start = time.time()
in_window = 0

with open(args.file, "r") as f:
    reader = csv.reader(f)
    next(reader)  # header: ,traceid,timestamp,rpcid,um,rpctype,dm,interface,rt
    for row in reader:
        try:
            if len(row) < 9:
                raise ValueError("short row")
            rt = float(row[8])
            if rt < 0:
                raise ValueError("negative rt")
            msg = {
                "traceid": row[1],
                "timestamp": int(row[2]),
                "rpcid": row[3],
                "um": row[4],
                "rpctype": row[5],
                "dm": row[6],
                "rt": rt,
            }
            if not msg["dm"] or not msg["traceid"]:
                raise ValueError("empty key field")
        except Exception:
            skipped += 1
            continue

        producer.send(args.topic, msg)
        sent += 1
        in_window += 1

        if in_window >= args.rate:
            elapsed = time.time() - window_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            window_start = time.time()
            in_window = 0

        if sent % 10000 == 0:
            print("sent={} skipped={}".format(sent, skipped))

producer.flush()
print("DONE sent={} skipped={}".format(sent, skipped))
```

## 3.3 Imports: what is used and why

### `argparse`

Reads options supplied when the script is launched. This avoids
hardcoding the file path, rate, topic, and broker address.

Example:

``` bash
python3 producer.py --file /root/MS_CallGraph_sample.csv --rate 800
```

### `csv`

Reads the dataset as CSV records. `csv.reader` returns each record as a
list of strings.

### `json`

Converts a Python dictionary to JSON text before it is encoded into
bytes for Kafka.

### `time`

Measures elapsed time and pauses execution when needed to regulate the
publishing rate.

### `KafkaProducer`

The Kafka client used to publish messages to a Kafka topic.

## 3.4 Command-line arguments

``` python
parser = argparse.ArgumentParser()
parser.add_argument("--file", required=True)
parser.add_argument("--rate", type=int, default=800)
parser.add_argument("--topic", default="microservices-logs")
parser.add_argument("--bootstrap", default="kafka:9092")
args = parser.parse_args()
```

-   `--file`: path to the CSV file; mandatory because `required=True`.
-   `--rate`: target number of messages per rate-control window;
    defaults to 800.
-   `--topic`: destination topic; defaults to `microservices-logs`.
-   `--bootstrap`: Kafka broker address; defaults to `kafka:9092`.
-   `parse_args()`: reads the supplied command-line arguments and stores
    them in `args`.

`type=int` converts the rate argument to an integer. `default` is used
when the caller does not provide an option.

## 3.5 Kafka producer configuration

``` python
producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20,
)
```

-   `bootstrap_servers` tells the producer which broker address to use.
-   `value_serializer` converts the Python message into bytes. Kafka
    transmits byte records, not Python dictionaries.
-   `json.dumps(v)` serializes the dictionary into JSON text.
-   `.encode("utf-8")` converts that text into UTF-8 bytes.
-   `linger_ms=20` allows the producer to wait briefly to accumulate
    records before sending a batch, which can improve batching
    efficiency.

The serialization path is:

``` text
Python dictionary -> JSON string -> UTF-8 bytes -> Kafka
```

Example dictionary:

``` python
{"traceid": "abc123", "rt": 541.0}
```

JSON text:

``` json
{"traceid": "abc123", "rt": 541.0}
```

## 3.6 Counters and rate-control variables

``` python
sent = 0
skipped = 0
window_start = time.time()
in_window = 0
```

-   `sent`: number of messages for which `producer.send()` has been
    called.
-   `skipped`: number of records rejected by validation.
-   `window_start`: start time of the current rate-control window.
-   `in_window`: number of messages sent in that window.

Important precision: `sent` counts calls queued through
`producer.send()`. It is not itself a count of broker acknowledgements.
`producer.flush()` waits for outstanding sends before the program
finishes.

## 3.7 Reading the CSV

``` python
with open(args.file, "r") as f:
    reader = csv.reader(f)
    next(reader)
```

-   `open(..., "r")` opens the file in read mode.
-   `with` is a context manager that closes the file when the block
    ends.
-   `csv.reader(f)` creates an iterator over CSV rows.
-   `next(reader)` consumes and skips the header row.

The code uses numeric positions in each row. Based on the supplied
header, the relevant positions are:

    Python index Field used
  -------------- -------------
               1 `traceid`
               2 `timestamp`
               3 `rpcid`
               4 `um`
               5 `rpctype`
               6 `dm`
               8 `rt`

Indexing starts at zero. Position 0 is the unnamed index column, and
position 7 (`interface`) is not included in the Kafka message.

## 3.8 Validation and exception handling

``` python
for row in reader:
    try:
        if len(row) < 9:
            raise ValueError("short row")
        rt = float(row[8])
        if rt < 0:
            raise ValueError("negative rt")
```

The code checks that:

1.  The row has enough columns.
2.  Response time can be converted to a floating-point number.
3.  Response time is not negative.

`raise ValueError(...)` deliberately raises an exception when a
validation condition fails.

The message dictionary is then constructed:

``` python
msg = {
    "traceid": row[1],
    "timestamp": int(row[2]),
    "rpcid": row[3],
    "um": row[4],
    "rpctype": row[5],
    "dm": row[6],
    "rt": rt,
}
```

This transforms the positional CSV record into a named, structured
Python dictionary. The timestamp is converted to `int`, and response
time is a `float`.

The code then checks two required identifiers:

``` python
if not msg["dm"] or not msg["traceid"]:
    raise ValueError("empty key field")
```

If validation fails:

``` python
except Exception:
    skipped += 1
    continue
```

-   `except Exception` catches exceptions raised in the validation
    block.
-   `skipped += 1` increments the rejected-record count.
-   `continue` skips the rest of the current loop iteration and moves to
    the next row.

## 3.9 Publishing and rate control

``` python
producer.send(args.topic, msg)
sent += 1
in_window += 1
```

`producer.send` publishes the message asynchronously to the selected
topic. The configured serializer converts the dictionary to bytes.

The rate-control block is:

``` python
if in_window >= args.rate:
    elapsed = time.time() - window_start
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    window_start = time.time()
    in_window = 0
```

When the number of queued messages reaches the configured rate, the
producer checks how much time has elapsed. If the window took less than
one second, it sleeps for the remaining time. It then starts a new
window.

This approximates the target publishing rate. Actual throughput can vary
because of processing time, network conditions, Kafka buffering, and
asynchronous delivery.

Progress is printed every 10,000 queued messages:

``` python
if sent % 10000 == 0:
    print("sent={} skipped={}".format(sent, skipped))
```

`%` is the modulo operator. The condition is true when `sent` is exactly
divisible by 10,000.

Finally:

``` python
producer.flush()
print("DONE sent={} skipped={}".format(sent, skipped))
```

`flush()` waits for outstanding producer sends to complete before the
script exits.

## 3.10 Key concepts to explain

-   **Producer:** a client that publishes records to Kafka.
-   **Topic:** a named stream of records; here, `microservices-logs`.
-   **Serialization:** converting a Python object into a transportable
    representation.
-   **Validation:** rejecting malformed records before publishing.
-   **Rate control:** regulating how quickly the producer queues
    records.
-   **Asynchronous send:** `send()` queues a message; `flush()` waits
    for outstanding sends.

------------------------------------------------------------------------

# 4. File 2 --- `streaming_job.py`

## 4.1 Purpose

`streaming_job.py` reads the `microservices-logs` Kafka topic using
Spark Structured Streaming. It parses JSON messages using an explicit
schema, identifies anomalies using a response-time threshold, and starts
three streaming queries:

1.  Print anomalies to the console.
2.  Archive all parsed records to HDFS in Parquet format.
3.  Publish anomalies to the Kafka topic `anomalies`.

## 4.2 Complete source code

``` python
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
```

## 4.3 Imports

### `sys`

Provides access to command-line arguments through `sys.argv`.

### `SparkSession`

The entry point for using Spark SQL and DataFrame functionality.

### `from_json`

Parses a JSON string column into a structured column using a supplied
schema.

### `col`

References a DataFrame column by name.

### `to_json`

Serializes a structured column into JSON text.

### `struct`

Combines multiple columns into one structured column. Here it is used to
build the JSON message sent to the anomalies topic.

### Schema types

-   `StructType`: defines the complete record schema.
-   `StructField`: defines one field in that schema.
-   `StringType`: represents text fields.
-   `DoubleType`: represents response time.
-   `LongType`: represents the integer timestamp.

## 4.4 Configurable anomaly threshold

``` python
THRESHOLD = float(sys.argv[1]) if len(sys.argv) > 1 else 500.0
```

This is a conditional expression.

-   If an argument is supplied, it is converted to `float` and used as
    the threshold.
-   If no argument is supplied, the threshold is `500.0`.

Examples:

``` bash
spark-submit ... /root/streaming_job.py 500
spark-submit ... /root/streaming_job.py 200
```

The first uses the normal 500 ms rule; the second uses the lower demo
threshold.

## 4.5 Creating the Spark session

``` python
spark = SparkSession.builder.appName("aiops-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
```

-   `builder` configures the Spark session.
-   `appName` gives the application a recognizable name.
-   `getOrCreate()` gets an existing session or creates one.
-   `setLogLevel("WARN")` reduces routine log output so warnings and
    errors are easier to notice.

## 4.6 Defining the JSON schema

``` python
schema = StructType([
    StructField("traceid", StringType()),
    StructField("rpcid", StringType()),
    StructField("um", StringType()),
    StructField("dm", StringType()),
    StructField("rpctype", StringType()),
    StructField("rt", DoubleType()),
    StructField("timestamp", LongType()),
])
```

Kafka values arrive as bytes. The schema tells Spark how to interpret
the fields after decoding and parsing the JSON.

Using an explicit schema avoids relying on schema inference for every
message.

Each `StructField` defines a field name and its expected data type.

## 4.7 Reading from Kafka

``` python
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "microservices-logs")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 5000)
       .load())
```

-   `readStream` creates a streaming DataFrame.
-   `format("kafka")` selects the Kafka source.
-   `kafka.bootstrap.servers` identifies the broker.
-   `subscribe` selects the topic.
-   `startingOffsets="earliest"` requests reading from the earliest
    available offsets for a new query without an existing checkpoint.
-   `maxOffsetsPerTrigger=5000` limits the number of offsets processed
    per trigger across the source.
-   `load()` creates the streaming DataFrame.

Kafka source records include metadata and byte columns, including `key`
and `value`. The project parses the `value` column.

## 4.8 Parsing the JSON

``` python
parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("d"))
             .select("d.*"))
```

The transformation has two stages:

1.  `col("value").cast("string")` converts Kafka's byte value into a
    string.
2.  `from_json(..., schema)` parses that JSON string using the schema
    and creates a structured column named `d`.
3.  `.select("d.*")` expands the fields inside `d` into individual
    DataFrame columns.

The resulting DataFrame has columns such as `traceid`, `rpcid`, `dm`,
and `rt`.

## 4.9 Detecting anomalies

``` python
anomalies = parsed.filter(col("rt") > THRESHOLD)
```

This filters the parsed DataFrame. Only records with response time
strictly greater than the threshold are included in `anomalies`.

For example, with a 500 ms threshold:

-   `rt = 450` is not an anomaly.
-   `rt = 500` is not an anomaly because the operator is `>`.
-   `rt = 501` is an anomaly.

## 4.10 Query 1: console output

``` python
q_console = (anomalies.select("traceid", "rpcid", "dm", "rt")
             .writeStream.format("console").outputMode("append").start())
```

This query prints selected anomaly fields to the Spark console.

-   `select` keeps only the columns useful for the test output.
-   `writeStream` starts defining a streaming sink.
-   `format("console")` selects the console sink.
-   `outputMode("append")` outputs newly added rows.
-   `start()` starts the query.

The README identifies this query as a testing/demo output and notes that
it has no checkpoint. Consequently, it can replay records after a
restart.

## 4.11 Query 2: archive all parsed records to HDFS

``` python
q_raw = (parsed.writeStream.format("parquet")
         .option("path", "hdfs://hadoop-master:9000/telemetry/raw")
         .option("checkpointLocation", "hdfs://hadoop-master:9000/telemetry/checkpoints/raw")
         .trigger(processingTime="10 seconds")
         .outputMode("append").start())
```

This query archives all parsed records, not only anomalies.

-   `format("parquet")` selects Parquet output.
-   `path` is the HDFS destination.
-   `checkpointLocation` stores streaming progress information.
-   `trigger(processingTime="10 seconds")` requests a processing trigger
    every 10 seconds.
-   `append` writes new records as they arrive.
-   `start()` starts the query.

The checkpoint allows Spark to recover progress and continue from
committed offsets after a restart, provided the same checkpoint is
reused and the query remains compatible.

## 4.12 Query 3: publish anomalies to another Kafka topic

First the output DataFrame is created:

``` python
out = anomalies.select(
    col("traceid").alias("key"),
    to_json(struct("traceid", "rpcid", "um", "dm", "rt", "timestamp")).alias("value"))
```

-   The trace identifier is selected as the Kafka key.
-   `struct(...)` combines the required anomaly fields.
-   `to_json(...)` converts the struct into JSON text.
-   The resulting columns are named `key` and `value`, as expected by
    the Kafka sink.

Then the Kafka sink is started:

``` python
q_kafka = (out.writeStream.format("kafka")
           .option("kafka.bootstrap.servers", "kafka:9092")
           .option("topic", "anomalies")
           .option("checkpointLocation", "hdfs://hadoop-master:9000/telemetry/checkpoints/anomalies")
           .outputMode("append").start())
```

This publishes anomaly messages to the `anomalies` topic. The separate
`kafka_to_hbase.py` process consumes that topic and writes the records
into HBase.

## 4.13 Waiting for the streaming queries

``` python
spark.streams.awaitAnyTermination()
```

The main program waits until one of the active streaming queries
terminates. This keeps the Spark application running instead of exiting
immediately after starting the queries.

## 4.14 Key concepts to explain

-   **Structured Streaming:** Spark's structured API for processing
    continuous data streams.
-   **Streaming DataFrame:** a DataFrame representing data arriving over
    time.
-   **Schema:** the names and data types expected in each JSON record.
-   **Filter:** keeps only rows satisfying a condition.
-   **Micro-batch:** a small group of records processed during one
    streaming trigger.
-   **Checkpoint:** persistent progress information used for recovery.
-   **Sink:** the destination to which a streaming query writes.
-   **Append mode:** outputs newly arriving rows.

------------------------------------------------------------------------

# 5. File 3 --- `kafka_to_hbase.py`

## 5.1 Purpose

`kafka_to_hbase.py` is a bridge between Kafka and HBase. It consumes
anomaly messages from the Kafka topic `anomalies`, converts each JSON
message into a Python dictionary, builds a row key, and writes the
record to the HBase table `anomalies`.

This bridge is used because the project's Spark version is 2.2, while
the `foreachBatch` streaming API was introduced in Spark 2.4. The
separate Python consumer provides the HBase-writing step.

## 5.2 Complete source code

``` python
import json
import happybase
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "anomalies",
    bootstrap_servers="kafka:9092",
    group_id="hbase-bridge",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
conn = happybase.Connection("localhost", 9090)
table = conn.table("anomalies")

total = 0
while True:
    records = consumer.poll(timeout_ms=1000, max_records=500)
    for tp, msgs in records.items():
        with table.batch() as b:
            for m in msgs:
                v = m.value
                rowkey = "{}#{}".format(v["traceid"], v["rpcid"]).encode()
                b.put(rowkey, {
                    b"metrics:dm": str(v["dm"]).encode(),
                    b"metrics:um": str(v["um"]).encode(),
                    b"metrics:rt": str(v["rt"]).encode(),
                    b"metrics:timestamp": str(v["timestamp"]).encode(),
                })
                total += 1
        print("written to HBase: {}".format(total))
```

## 5.3 Imports

-   `json`: parses JSON message bytes into Python objects.
-   `happybase`: connects to HBase through its Thrift server and
    performs table operations.
-   `KafkaConsumer`: reads messages from Kafka.

## 5.4 Creating the Kafka consumer

``` python
consumer = KafkaConsumer(
    "anomalies",
    bootstrap_servers="kafka:9092",
    group_id="hbase-bridge",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
```

-   `"anomalies"` is the Kafka topic to consume.
-   `bootstrap_servers` identifies the Kafka broker.
-   `group_id="hbase-bridge"` identifies the consumer group.
-   `auto_offset_reset="earliest"` tells a new consumer group with no
    committed offset where to begin: at the earliest available offset.
-   `value_deserializer` decodes the bytes as UTF-8 and parses the JSON
    into a Python object.

The deserialization path is:

``` text
Kafka bytes -> UTF-8 string -> Python dictionary
```

A consumer group allows Kafka to track consumption progress for that
group. The `auto_offset_reset` setting applies when there is no valid
committed offset for the group; it does not mean every restart always
replays from the beginning.

## 5.5 Connecting to HBase

``` python
conn = happybase.Connection("localhost", 9090)
table = conn.table("anomalies")
```

The Python process connects to the HBase Thrift service on port 9090 and
obtains a handle to the `anomalies` table.

In this setup, the bridge runs inside the Hadoop master container, so
`localhost` refers to that container.

## 5.6 Polling Kafka

``` python
total = 0
while True:
    records = consumer.poll(timeout_ms=1000, max_records=500)
```

-   `total` tracks how many messages have been written during this
    process run.
-   `while True` creates a continuous consumer loop.
-   `poll` asks Kafka for available records.
-   `timeout_ms=1000` allows the poll to wait up to one second.
-   `max_records=500` limits the number of returned records in a poll.

The returned records are grouped by topic-partition:

``` python
for tp, msgs in records.items():
```

`tp` represents a topic-partition, and `msgs` is the list of messages
returned for that partition.

## 5.7 Batch-writing to HBase

``` python
with table.batch() as b:
    for m in msgs:
        v = m.value
```

`table.batch()` creates a batch writer. Grouping writes reduces the
overhead of issuing a separate operation for every record.

`m.value` is already a Python dictionary because the consumer's
`value_deserializer` parsed the JSON.

## 5.8 Constructing the row key

``` python
rowkey = "{}#{}".format(v["traceid"], v["rpcid"]).encode()
```

The row key combines the trace ID and RPC ID.

Example:

``` text
traceid = abc123
rpcid   = 0.1.2
row key = abc123#0.1.2
```

This identifies a call within a trace. The README explains that using
the same key for a replayed call causes the same HBase row to be
overwritten rather than creating a second row.

## 5.9 Writing columns

``` python
b.put(rowkey, {
    b"metrics:dm": str(v["dm"]).encode(),
    b"metrics:um": str(v["um"]).encode(),
    b"metrics:rt": str(v["rt"]).encode(),
    b"metrics:timestamp": str(v["timestamp"]).encode(),
})
```

HBase stores values as bytes. The code converts each value to text and
then encodes it.

The column names use the form `family:qualifier`:

-   `metrics:dm`: downstream service.
-   `metrics:um`: upstream service.
-   `metrics:rt`: response time.
-   `metrics:timestamp`: timestamp from the original data.

The HBase table's column family is `metrics`; the qualifiers are `dm`,
`um`, `rt`, and `timestamp`.

The count is incremented:

``` python
total += 1
```

After processing the partition's messages, the program prints the
running total.

## 5.10 Key concepts to explain

-   **Consumer:** reads records from a Kafka topic.
-   **Consumer group:** identifies a group of consumers whose offsets
    are tracked together.
-   **Polling:** retrieving available records from Kafka.
-   **Deserialization:** converting bytes back into a structured Python
    object.
-   **HBase row key:** identifies a row.
-   **Column family:** a group of related columns in HBase.
-   **Batch write:** groups multiple writes to reduce overhead.
-   **Idempotent row-key effect:** writing the same values under the
    same row key overwrites the same row rather than adding another row.

------------------------------------------------------------------------

# 6. File 4 --- `batch_job.py`

## 6.1 Purpose

`batch_job.py` reads the Parquet archive created by Spark Structured
Streaming. It excludes records with an unknown downstream service from
per-service calculations, groups the remaining records by downstream
service, calculates historical latency metrics, and writes those metrics
to HDFS and HBase.

This is the historical analytics path of the project.

## 6.2 Complete source code

``` python
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
```

## 6.3 Imports

-   `happybase`: writes the aggregated service metrics to HBase.
-   `SparkSession`: creates the Spark application and reads the Parquet
    archive.
-   `functions as F`: imports Spark SQL functions under the shorter
    alias `F`.

Examples of functions accessed through `F` include `F.col`, `F.count`,
`F.avg`, `F.round`, `F.max`, `F.sum`, `F.when`, and `F.desc`.

## 6.4 Configuration and Spark session

``` python
THRESHOLD = 500.0
BASE = "hdfs://hadoop-master:9000/telemetry"

spark = SparkSession.builder.appName("aiops-batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
```

-   `THRESHOLD` defines the threshold used for the historical
    `slow_calls` metric.
-   `BASE` stores the common HDFS directory.
-   The Spark session is named `aiops-batch`.
-   The log level is set to `WARN`.

The threshold is explicitly 500 ms in this batch file, even if the live
demonstration uses 200 ms in the streaming job.

## 6.5 Reading the archive

``` python
df = spark.read.parquet(BASE + "/raw")
print("raw rows in archive:", df.count())
```

-   `spark.read.parquet(...)` reads the Parquet archive from HDFS.
-   `df` is the resulting Spark DataFrame.
-   `count()` triggers a Spark action and returns the number of rows.

The archive contains all parsed records written by the raw streaming
query, not only anomalies.

## 6.6 Cleaning records for service statistics

``` python
clean = df.filter((F.col("dm") != "(?)") & (F.col("rt") >= 0))
```

This keeps rows satisfying both conditions:

1.  `dm` is not the unknown-service placeholder `(?)`.
2.  `rt` is nonnegative.

The `&` operator combines the two Boolean conditions in a Spark
DataFrame filter.

The unknown-service placeholder is excluded because it cannot be
attributed to a known downstream service. The raw archive itself is not
deleted or changed by this filter.

## 6.7 Grouping and aggregating

``` python
stats = (clean.groupBy("dm").agg(
    F.count("*").alias("calls"),
    F.round(F.avg("rt"), 2).alias("avg_rt"),
    F.expr("percentile_approx(rt, 0.95)").alias("p95_rt"),
    F.max("rt").alias("max_rt"),
    F.sum(F.when(F.col("rt") > THRESHOLD, 1).otherwise(0)).alias("slow_calls"),
))
```

This is the central analytical operation.

### `groupBy("dm")`

Groups records by downstream service. Each group represents one service.

### `F.count("*").alias("calls")`

Counts the records in each service group. The output column is named
`calls`.

### `F.avg("rt")`

Calculates the average response time for the service.

### `F.round(..., 2)`

Rounds the average to two decimal places.

### `F.expr("percentile_approx(rt, 0.95)")`

Calculates an approximate 95th percentile of response time. The p95 is a
latency value below or equal to which approximately 95% of observations
fall.

It is approximate rather than an exact percentile calculation.

### `F.max("rt")`

Finds the maximum response time in the service group.

### `F.when(...).otherwise(0)`

``` python
F.when(F.col("rt") > THRESHOLD, 1).otherwise(0)
```

Returns 1 when a call exceeds 500 ms and 0 otherwise.

### `F.sum(...)`

Adds those 1 and 0 values to calculate the number of slow calls for each
service.

The resulting DataFrame contains:

  Column         Meaning
  -------------- -------------------------------------------
  `dm`           Downstream service identifier
  `calls`        Total calls for the service
  `avg_rt`       Average response time
  `p95_rt`       Approximate 95th-percentile response time
  `max_rt`       Maximum response time
  `slow_calls`   Number of calls with `rt > 500 ms`

## 6.8 Cache and count

``` python
stats.cache()
print("services:", stats.count())
```

`cache()` marks the DataFrame for caching so Spark can reuse its
computed result for later operations. `count()` triggers computation and
returns the number of service groups.

Caching is useful because the same `stats` result is subsequently
written to HDFS, collected for HBase, and displayed.

## 6.9 Writing metrics to HDFS

``` python
(stats.coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(BASE + "/metrics"))
```

-   `coalesce(1)` reduces the output to one partition, resulting in one
    data part file for this output.
-   `write` starts a batch write.
-   `mode("overwrite")` replaces existing output at the target path.
-   `option("header", "true")` writes column names in the CSV output.
-   `csv(...)` writes the metrics to HDFS.

This is a Spark directory output; it should not be assumed to be a
single file named `metrics.csv`.

## 6.10 Writing metrics to HBase

``` python
rows = stats.collect()
conn = happybase.Connection("localhost", 9090)
table = conn.table("service_metrics")
```

`collect()` brings the aggregated result rows from Spark executors to
the driver as a local collection.

The code comments that this is suitable because the result is small: one
row per service, rather than one row per original call.

The process connects to HBase and selects the `service_metrics` table.

The batch write is:

``` python
with table.batch(batch_size=500) as b:
    for r in rows:
        b.put(r["dm"].encode(), {
            b"stats:calls": str(r["calls"]).encode(),
            b"stats:avg_rt": str(r["avg_rt"]).encode(),
            b"stats:p95_rt": str(r["p95_rt"]).encode(),
            b"stats:max_rt": str(r["max_rt"]).encode(),
            b"stats:slow_calls": str(r["slow_calls"]).encode(),
        })
```

-   `batch_size=500` configures the HappyBase batch writer.
-   The downstream service ID (`dm`) becomes the HBase row key.
-   Each metric is stored under the `stats` column family.
-   Values are converted to strings and encoded into bytes.

## 6.11 Displaying the top services

``` python
stats.orderBy(F.desc("slow_calls")).show(10, False)
```

-   `orderBy` sorts the service groups.
-   `F.desc("slow_calls")` sorts in descending order, placing the
    largest slow-call count first.
-   `show(10, False)` displays ten rows without truncating long column
    values.

## 6.12 Key concepts to explain

-   **Batch processing:** processing a stored dataset rather than
    handling each new record immediately.
-   **GroupBy:** groups records with the same key.
-   **Aggregation:** calculates summary values for each group.
-   **Average:** mean response time.
-   **p95:** approximate 95th-percentile response time.
-   **Cache:** keeps a computed DataFrame available for reuse.
-   **Collect:** transfers results to the Spark driver.
-   **Coalesce:** reduces the number of output partitions.
-   **Overwrite:** replaces the existing output directory contents.

------------------------------------------------------------------------

# 7. File 5 --- `api.py`

## 7.1 Purpose

`api.py` runs on the Windows host and exposes HBase data through HTTP
endpoints using FastAPI.

It provides:

-   A health endpoint that checks HBase connectivity and lists tables.
-   An endpoint that returns recent anomaly records.
-   An endpoint that returns service metrics, with sorting and result
    limits.

The dashboard calls these endpoints instead of connecting directly to
HBase.

## 7.2 Complete source code

``` python
import happybase
from fastapi import FastAPI, Query

app = FastAPI(title="AIOps API")
HBASE_HOST = "localhost"
HBASE_PORT = 9090


def conn():
    return happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000)


def val(data, col, default=b""):
    return data.get(col, default).decode()


@app.get("/api/health")
def health():
    try:
        c = conn()
        tables = [t.decode() for t in c.tables()]
        c.close()
        return {"status": "ok", "tables": tables}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/api/anomalies/live")
def anomalies(limit: int = Query(50, ge=1, le=500)):
    c = conn()
    table = c.table("anomalies")
    rows = []
    for key, d in table.scan(limit=5000):
        k = key.decode()
        traceid, _, rpcid = k.partition("#")
        rows.append({
            "traceid": traceid,
            "rpcid": rpcid,
            "um": val(d, b"metrics:um"),
            "dm": val(d, b"metrics:dm"),
            "rt": float(val(d, b"metrics:rt", b"0")),
            "timestamp": int(val(d, b"metrics:timestamp", b"0")),
        })
    c.close()
    rows.sort(key=lambda r: r["timestamp"], reverse=True)
    return {"count": len(rows), "items": rows[:limit]}


@app.get("/api/services/stats")
def stats(limit: int = Query(20, ge=1, le=500),
          sort_by: str = Query("slow_calls")):
    c = conn()
    table = c.table("service_metrics")
    rows = []
    for key, d in table.scan():
        rows.append({
            "service": key.decode(),
            "calls": int(val(d, b"stats:calls", b"0")),
            "avg_rt": float(val(d, b"stats:avg_rt", b"0")),
            "p95_rt": float(val(d, b"stats:p95_rt", b"0")),
            "max_rt": float(val(d, b"stats:max_rt", b"0")),
            "slow_calls": int(val(d, b"stats:slow_calls", b"0")),
        })
    c.close()
    if sort_by not in ("calls", "avg_rt", "p95_rt", "max_rt", "slow_calls"):
        sort_by = "slow_calls"
    rows.sort(key=lambda r: r[sort_by], reverse=True)
    return {"total_services": len(rows), "items": rows[:limit]}
```

## 7.3 Imports

### `happybase`

Provides the Python connection to HBase through Thrift. The API uses it
to scan HBase tables.

### `FastAPI`

Creates the HTTP API application and maps Python functions to routes.

### `Query`

Defines and validates query parameters in endpoint functions.

## 7.4 Application configuration

``` python
app = FastAPI(title="AIOps API")
HBASE_HOST = "localhost"
HBASE_PORT = 9090
```

-   `app` is the FastAPI application object.
-   `title` gives the API its name in the generated API documentation.
-   `HBASE_HOST` and `HBASE_PORT` identify the Thrift service.

Because the API runs on the Windows host, `localhost:9090` refers to the
host's published Thrift port.

## 7.5 Connection helper

``` python
def conn():
    return happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000)
```

This function centralizes HBase connection creation. Instead of
repeating the host and port in every endpoint, the endpoints call
`conn()`.

`timeout=10000` sets a 10,000 millisecond timeout.

## 7.6 Value helper

``` python
def val(data, col, default=b""):
    return data.get(col, default).decode()
```

HBase values are returned as bytes. This helper:

1.  Looks up the requested column in the row dictionary.
2.  Uses the default byte value if the column is missing.
3.  Decodes the bytes into a string.

For example, a stored byte value such as `b"541.0"` becomes the string
`"541.0"`.

The endpoints then convert numeric values to `float` or `int`.

## 7.7 Health endpoint

``` python
@app.get("/api/health")
def health():
    try:
        c = conn()
        tables = [t.decode() for t in c.tables()]
        c.close()
        return {"status": "ok", "tables": tables}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
```

`@app.get("/api/health")` registers the function as a GET endpoint.

The function:

1.  Opens an HBase connection.
2.  Retrieves the table names using `c.tables()`.
3.  Decodes the table names from bytes.
4.  Closes the connection.
5.  Returns a JSON-compatible dictionary.

If an exception occurs, it returns an error status and the exception
text.

## 7.8 Live anomalies endpoint

``` python
@app.get("/api/anomalies/live")
def anomalies(limit: int = Query(50, ge=1, le=500)):
```

This endpoint accepts a `limit` query parameter.

-   Default: 50.
-   Minimum: 1.
-   Maximum: 500.

For example:

``` text
/api/anomalies/live?limit=100
```

The endpoint selects the HBase table:

``` python
c = conn()
table = c.table("anomalies")
rows = []
```

It scans up to 5,000 rows:

``` python
for key, d in table.scan(limit=5000):
```

For each HBase row:

``` python
k = key.decode()
traceid, _, rpcid = k.partition("#")
```

The row key has the form `traceid#rpcid`. `partition("#")` splits it at
the first `#` and returns three values: the text before the separator,
the separator, and the text after it.

The endpoint builds a dictionary containing:

-   `traceid`
-   `rpcid`
-   `um`
-   `dm`
-   `rt`
-   `timestamp`

The helper `val()` decodes HBase byte values. `float()` and `int()`
convert numeric strings to Python numeric values.

The connection is closed, then rows are sorted:

``` python
rows.sort(key=lambda r: r["timestamp"], reverse=True)
```

This sorts by timestamp, newest first.

The response is:

``` python
return {"count": len(rows), "items": rows[:limit]}
```

`rows[:limit]` is Python slicing. It returns only the first `limit`
records.

Important design detail from the README: HBase scans are ordered by row
key, not by the original event timestamp. The API sorts the scanned rows
by timestamp itself. The endpoint scans at most 5,000 rows before
sorting, so it does not necessarily identify the newest records across
an arbitrarily large table.

## 7.9 Service statistics endpoint

``` python
@app.get("/api/services/stats")
def stats(limit: int = Query(20, ge=1, le=500),
          sort_by: str = Query("slow_calls")):
```

This endpoint accepts:

-   `limit`: number of service rows to return; default 20, range 1--500.
-   `sort_by`: metric used to sort; default `slow_calls`.

It scans the `service_metrics` HBase table:

``` python
c = conn()
table = c.table("service_metrics")
rows = []
for key, d in table.scan():
```

Each row becomes a dictionary containing the service identifier and the
metrics calculated by `batch_job.py`.

The endpoint then validates the requested sort field:

``` python
if sort_by not in ("calls", "avg_rt", "p95_rt", "max_rt", "slow_calls"):
    sort_by = "slow_calls"
```

If the requested field is not allowed, it falls back to `slow_calls`.

Then:

``` python
rows.sort(key=lambda r: r[sort_by], reverse=True)
return {"total_services": len(rows), "items": rows[:limit]}
```

The service rows are sorted in descending order and limited before being
returned.

## 7.10 Key concepts to explain

-   **REST API:** an HTTP interface that exposes resources through
    endpoints.
-   **GET:** an HTTP method used to retrieve data.
-   **Endpoint/route:** a URL handled by an API function.
-   **Query parameter:** a value supplied in the URL, such as
    `?limit=50`.
-   **Validation:** FastAPI checks query parameter constraints.
-   **JSON response:** dictionaries returned by FastAPI are serialized
    into JSON.
-   **Separation of concerns:** the dashboard calls the API rather than
    implementing HBase access itself.

------------------------------------------------------------------------

# 8. File 6 --- `dashboard.py`

## 8.1 Purpose

`dashboard.py` creates the user-facing dashboard with Streamlit. It
calls the FastAPI endpoints, displays summary metrics, visualizes live
anomalies, displays a latest-anomalies table, and compares batch latency
metrics for services.

The live section can refresh every two seconds.

## 8.2 Complete source code

``` python
import pandas as pd
import requests
import altair as alt
import streamlit as st

API = "http://127.0.0.1:8010"
THRESHOLD = 500

st.set_page_config(page_title="AIOps Root-Cause Dashboard", layout="wide")
st.title("AIOps: microservice slow-call detection")

limit = st.sidebar.slider("Anomalies shown in table", 10, 200, 50)
auto = st.sidebar.checkbox("Auto-refresh every 2 s", True)
st.sidebar.caption("Rule A: anomalous if rt > {} ms (demo stream uses 200 ms)".format(THRESHOLD))


def get(path, **params):
    try:
        r = requests.get(API + path, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error("API error on {}: {}".format(path, e))
        return None


@st.fragment(run_every=2 if auto else None)
def live():
    an = get("/api/anomalies/live", limit=500)
    stt = get("/api/services/stats", limit=15, sort_by="slow_calls")
    if an is None or stt is None:
        return

    items = an["items"]
    c1, c2, c3 = st.columns(3)
    c1.metric("Anomalies in HBase (live)", an["count"])
    c2.metric("Services profiled (batch)", stt["total_services"])
    c3.metric("Worst rt in view (ms)", max([i["rt"] for i in items]) if items else 0)

    if items:
        df = pd.DataFrame(items)
        df["service"] = df["dm"].str[:8]

        left, right = st.columns(2)
        with left:
            st.subheader("LIVE: anomalies per service (last 500)")
            cnt = df.groupby("service").size().reset_index(name="anomalies")
            cnt = cnt.sort_values("anomalies", ascending=False).head(10)
            st.altair_chart(alt.Chart(cnt).mark_bar().encode(
                x=alt.X("service:N", sort="-y"), y="anomalies:Q",
                tooltip=["service", "anomalies"]).properties(height=300))
        with right:
            st.subheader("LIVE: anomaly rt over time (last 500)")
            st.altair_chart(alt.Chart(df).mark_circle(size=40).encode(
                x=alt.X("timestamp:Q", title="data timestamp"),
                y=alt.Y("rt:Q", title="rt (ms)"),
                color=alt.Color("service:N", legend=None),
                tooltip=["service", "rpcid", "rt"]).properties(height=300))

        st.subheader("Latest anomalies")
        t = df.head(limit).copy()
        for col in ("traceid", "um", "dm"):
            t[col] = t[col].str[:12]
        st.dataframe(t[["traceid", "rpcid", "um", "dm", "rt", "timestamp"]],
                     hide_index=True)
    else:
        st.info("No anomalies yet.")

    st.subheader("BATCH: usual latency of the riskiest services (updates when batch_job runs)")
    s = pd.DataFrame(stt["items"])
    if not s.empty:
        s["label"] = s["service"].str[:8]
        long = s.melt(id_vars=["label", "slow_calls"],
                      value_vars=["avg_rt", "p95_rt"],
                      var_name="metric", value_name="ms")
        bars = alt.Chart(long).mark_bar().encode(
            x=alt.X("label:N", sort=None, title="service"),
            y=alt.Y("ms:Q", title="latency (ms)"),
            color="metric:N", xOffset="metric:N",
            tooltip=["label", "metric", "ms", "slow_calls"])
        rule = alt.Chart(pd.DataFrame({"y": [THRESHOLD]})).mark_rule(
            color="red", strokeDash=[6, 4]).encode(y="y:Q")
        st.altair_chart((bars + rule).properties(height=300, width=900))


live()
```

## 8.3 Imports

### `pandas`

Used to convert API JSON records into DataFrames and manipulate tabular
data.

### `requests`

Used to send HTTP GET requests to FastAPI.

### `altair`

Used to build the charts.

### `streamlit`

Used to build the dashboard interface, controls, metrics, tables, and
refresh behavior.

## 8.4 API address and threshold

``` python
API = "http://127.0.0.1:8010"
THRESHOLD = 500
```

The API is expected to run on the Windows host at port 8010. The
threshold constant is used for the batch latency reference line and
caption.

The caption mentions that the demo stream uses 200 ms, while this
dashboard constant remains 500. This is a distinction between the normal
rule and the demo configuration.

## 8.5 Page setup and sidebar controls

``` python
st.set_page_config(page_title="AIOps Root-Cause Dashboard", layout="wide")
st.title("AIOps: microservice slow-call detection")
```

-   `set_page_config` sets page metadata and layout.
-   `layout="wide"` uses a wide page layout.
-   `title` displays the main dashboard title.

The sidebar controls are:

``` python
limit = st.sidebar.slider("Anomalies shown in table", 10, 200, 50)
auto = st.sidebar.checkbox("Auto-refresh every 2 s", True)
```

-   The slider lets the user select how many anomaly rows to display in
    the table, from 10 to 200, initially 50.
-   The checkbox enables or disables automatic refresh, initially
    enabled.

The caption displays the threshold explanation.

## 8.6 API helper function

``` python
def get(path, **params):
    try:
        r = requests.get(API + path, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error("API error on {}: {}".format(path, e))
        return None
```

This function avoids repeating HTTP request and error-handling code.

-   `path` is the endpoint path.
-   `**params` collects any named query parameters into a dictionary.
-   `requests.get` sends a GET request to the API.
-   `params=params` lets Requests build the URL query string.
-   `timeout=10` sets a request timeout of 10 seconds.
-   `raise_for_status()` raises an exception for an unsuccessful HTTP
    status.
-   `r.json()` parses the JSON response into Python objects.
-   On an exception, Streamlit displays an error and the function
    returns `None`.

Example call:

``` python
get("/api/anomalies/live", limit=500)
```

This requests:

``` text
http://127.0.0.1:8010/api/anomalies/live?limit=500
```

## 8.7 Live fragment and automatic refresh

``` python
@st.fragment(run_every=2 if auto else None)
def live():
```

`@st.fragment` marks a function as a Streamlit fragment. The `run_every`
parameter schedules the fragment to rerun at the specified interval
while the app session is active.

-   If `auto` is true, the fragment refreshes every 2 seconds.
-   If `auto` is false, `run_every` is `None`, so there is no timed
    refresh.

The function retrieves both views:

``` python
an = get("/api/anomalies/live", limit=500)
stt = get("/api/services/stats", limit=15, sort_by="slow_calls")
```

-   `an` contains anomaly data.
-   `stt` contains service metrics.

If either request fails, the function returns without trying to display
missing data.

## 8.8 Summary metrics

``` python
items = an["items"]
c1, c2, c3 = st.columns(3)
c1.metric("Anomalies in HBase (live)", an["count"])
c2.metric("Services profiled (batch)", stt["total_services"])
c3.metric("Worst rt in view (ms)", max([i["rt"] for i in items]) if items else 0)
```

`st.columns(3)` creates three columns.

The dashboard displays:

1.  Number of anomalies returned by the API's scan.
2.  Number of services with batch metrics.
3.  Maximum response time among the returned anomaly records, or zero if
    the list is empty.

The third value is the maximum in the current returned view, not
necessarily the maximum across the entire HBase table.

## 8.9 Preparing anomaly data with Pandas

``` python
if items:
    df = pd.DataFrame(items)
    df["service"] = df["dm"].str[:8]
```

-   `pd.DataFrame(items)` converts the list of dictionaries into a
    DataFrame.
-   `df["dm"].str[:8]` takes the first eight characters of each
    downstream service identifier.
-   The shortened identifier is stored in a new `service` column for
    more readable chart labels.

## 8.10 Live bar chart: anomalies per service

``` python
cnt = df.groupby("service").size().reset_index(name="anomalies")
cnt = cnt.sort_values("anomalies", ascending=False).head(10)
```

-   `groupby("service")` groups anomaly records by shortened service ID.
-   `size()` counts records in each group.
-   `reset_index(name="anomalies")` converts the grouped result back to
    a DataFrame with an `anomalies` column.
-   `sort_values(..., ascending=False)` places the largest count first.
-   `head(10)` keeps the top ten.

Altair then builds a bar chart:

``` python
alt.Chart(cnt).mark_bar().encode(
    x=alt.X("service:N", sort="-y"),
    y="anomalies:Q",
    tooltip=["service", "anomalies"]
).properties(height=300)
```

-   `mark_bar()` selects bars.
-   `service:N` means the x-axis is nominal/categorical.
-   `anomalies:Q` means the y-axis is quantitative/numeric.
-   `tooltip` specifies information shown when hovering over a bar.
-   `properties(height=300)` sets chart height.

## 8.11 Live scatter plot: response time over time

``` python
alt.Chart(df).mark_circle(size=40).encode(
    x=alt.X("timestamp:Q", title="data timestamp"),
    y=alt.Y("rt:Q", title="rt (ms)"),
    color=alt.Color("service:N", legend=None),
    tooltip=["service", "rpcid", "rt"]
).properties(height=300)
```

This chart displays one circle per anomaly record.

-   x-axis: event timestamp.
-   y-axis: response time in milliseconds.
-   color: shortened service identifier.
-   tooltip: service, RPC identifier, and response time.

It helps show how anomaly response times vary across the data
timestamps.

## 8.12 Latest anomalies table

``` python
t = df.head(limit).copy()
for col in ("traceid", "um", "dm"):
    t[col] = t[col].str[:12]
st.dataframe(t[["traceid", "rpcid", "um", "dm", "rt", "timestamp"]],
             hide_index=True)
```

-   `head(limit)` selects the first `limit` rows.
-   `copy()` creates a separate DataFrame for display transformations.
-   The loop shortens long identifiers to 12 characters.
-   `st.dataframe` displays the selected columns.
-   `hide_index=True` hides the DataFrame index.

If there are no anomalies, the dashboard displays:

``` python
st.info("No anomalies yet.")
```

## 8.13 Batch latency chart

The dashboard next retrieves the service metrics already produced by
`batch_job.py`.

``` python
s = pd.DataFrame(stt["items"])
```

The service identifiers are shortened:

``` python
s["label"] = s["service"].str[:8]
```

The data is reshaped:

``` python
long = s.melt(id_vars=["label", "slow_calls"],
              value_vars=["avg_rt", "p95_rt"],
              var_name="metric", value_name="ms")
```

`melt` changes the data from a wide layout, with separate `avg_rt` and
`p95_rt` columns, into a long layout with:

-   `label`: service identifier.
-   `slow_calls`: number of slow calls.
-   `metric`: either `avg_rt` or `p95_rt`.
-   `ms`: the latency value.

Altair uses the reshaped data to draw grouped bars:

``` python
bars = alt.Chart(long).mark_bar().encode(
    x=alt.X("label:N", sort=None, title="service"),
    y=alt.Y("ms:Q", title="latency (ms)"),
    color="metric:N", xOffset="metric:N",
    tooltip=["label", "metric", "ms", "slow_calls"])
```

This compares average latency and p95 latency for each service.

A horizontal threshold rule is created:

``` python
rule = alt.Chart(pd.DataFrame({"y": [THRESHOLD]})).mark_rule(
    color="red", strokeDash=[6, 4]).encode(y="y:Q")
```

The final chart combines the bars and threshold line:

``` python
st.altair_chart((bars + rule).properties(height=300, width=900))
```

The batch chart updates when `batch_job.py` is run again and writes new
service metrics. The dashboard refresh alone does not recalculate the
batch statistics.

## 8.14 Starting the fragment

``` python
live()
```

This calls the function and renders the dashboard fragment.

## 8.15 Key concepts to explain

-   **Streamlit:** Python framework for interactive web dashboards.
-   **Requests:** sends HTTP requests to the API.
-   **Pandas DataFrame:** tabular in-memory representation of API data.
-   **GroupBy:** groups rows by a shared value.
-   **Melt:** reshapes data from wide to long format.
-   **Altair:** declarative charting library.
-   **Fragment:** allows a portion of the Streamlit app to rerun
    independently.
-   **Metric:** a dashboard component that displays a key value.

------------------------------------------------------------------------

# 9. How the six files communicate

This section connects the individual implementations into one execution
flow.

## 9.1 Ingestion

`producer.py` reads the CSV and publishes JSON messages to Kafka:

``` text
CSV -> Python dictionary -> JSON bytes -> microservices-logs
```

## 9.2 Stream processing

`streaming_job.py` reads the topic and parses the records:

``` text
microservices-logs -> Spark -> parsed DataFrame
```

It then creates two important outputs:

-   All parsed rows are archived in HDFS as Parquet.
-   Rows where `rt > THRESHOLD` are published to the `anomalies` topic.

## 9.3 Live anomaly storage

`kafka_to_hbase.py` consumes the anomaly topic:

``` text
anomalies topic -> KafkaConsumer -> JSON dictionary -> HBase anomalies
```

The row key is `traceid#rpcid`.

## 9.4 Historical analytics

`batch_job.py` reads the Parquet archive:

``` text
HDFS Parquet -> Spark DataFrame -> groupBy(dm) -> service metrics
```

It writes the aggregated result to:

-   HDFS `/telemetry/metrics`
-   HBase `service_metrics`

## 9.5 API layer

`api.py` reads the two HBase tables and exposes their content:

``` text
HBase -> HappyBase -> FastAPI -> JSON HTTP responses
```

## 9.6 Dashboard layer

`dashboard.py` calls the API:

``` text
FastAPI -> Requests -> Pandas -> Altair / Streamlit
```

The dashboard combines live anomaly data and batch service metrics.

------------------------------------------------------------------------

# 10. Execution order

The README specifies the following general order. Commands below are
organized by where they run.

## 10.1 Start the API on Windows

In a Windows PowerShell window, from the project directory:

``` powershell
python -m uvicorn api:app --host 127.0.0.1 --port 8010 --ws none
```

## 10.2 Start the dashboard on Windows

In another PowerShell window:

``` powershell
python -m streamlit run dashboard.py --server.port 8502
```

## 10.3 Start Spark streaming inside the container

In the Hadoop master container:

``` bash
export PYSPARK_PYTHON=python3
spark-submit --master local[2] --driver-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
  /root/streaming_job.py 200
```

Use `500` instead of `200` to use the normal anomaly rule.

## 10.4 Start the Kafka-to-HBase bridge

In another container shell:

``` bash
python3 /root/kafka_to_hbase.py
```

## 10.5 Start the producer

In another container shell:

``` bash
python3 /root/producer.py --file /root/MS_CallGraph_sample.csv --rate 800
```

## 10.6 Run the batch job after the archive catches up

``` bash
spark-submit --master local[2] --driver-memory 1g /root/batch_job.py
```

The batch job should be run after the streaming archive has caught up
with the producer. Otherwise, the historical metrics will be calculated
from a partial archive.

------------------------------------------------------------------------

# 11. Important implementation details and limitations

These points are useful for a technically honest oral defense.

1.  **The data is replayed, not live production traffic.** The project
    simulates a stream by publishing records from a real trace dataset.
2.  **The anomaly rule is simple.** `rt > 500 ms` is explainable, but a
    service-specific percentile threshold could adapt better to each
    service's normal behavior.
3.  **The demo uses 200 ms.** This produces more visible anomalies than
    the 500 ms rule.
4.  **Negative response times are skipped by the producer.** The README
    reports that 60,734 of the first 300,000 rows had negative `rt`.
5.  **Unknown downstream services are retained in the raw archive.**
    They are excluded from per-service statistics because `(?)` cannot
    identify a service to blame.
6.  **Service identifiers are anonymized.** The dashboard shortens
    hashes for readability.
7.  **Spark 2.2 does not provide `foreachBatch`.** The separate Kafka
    consumer bridge writes anomaly records to HBase.
8.  **The console query has no checkpoint.** The archive and anomaly
    Kafka sink have checkpoint locations; the console query does not.
9.  **The raw archive has a 5,000-offset-per-trigger limit and a
    10-second trigger.** It can lag behind the producer. Wait for it to
    catch up before running the batch job.
10. **HBase scans are by row key, not event timestamp.** The API sorts
    the rows it scans by timestamp. The live endpoint scans up to 5,000
    rows, so this is not a scalable global newest-first query strategy.
11. **The batch job uses `collect()`.** This transfers all aggregated
    service rows to the Spark driver. It is reasonable for the small
    result in this project, but collecting a very large result could
    exhaust driver memory.
12. **The dashboard's live data and batch metrics have different update
    behavior.** The live fragment refreshes every two seconds; batch
    metrics change when the batch job is run again.

------------------------------------------------------------------------

# 12. Oral defense questions and answers

## Architecture and general concepts

### Q1. What problem does your project solve?

It demonstrates a microservice monitoring pipeline that detects slow
calls and helps identify downstream services that may be responsible for
latency.

### Q2. What does AIOps mean in this project?

It refers to applying data processing and analytics to operational
telemetry. This implementation detects latency anomalies and presents
information that can support root-cause analysis.

### Q3. Why do you need both a speed layer and a batch layer?

The speed layer detects slow calls while the stream is being processed.
The batch layer calculates historical behavior per service. Comparing
them helps distinguish unusual spikes from a service's normal latency
profile.

### Q4. Is the stream real-time?

It is a replay of real trace records, not live production traffic. The
producer publishes records continuously to simulate a stream.

## Kafka and producer

### Q5. What is Kafka?

Kafka is a distributed event-streaming platform. In this project it acts
as a message broker between the CSV producer and Spark.

### Q6. What is a Kafka topic?

A topic is a named stream of records. The project uses
`microservices-logs` for input records and `anomalies` for detected
anomalies.

### Q7. Why serialize messages as JSON?

JSON gives the message a structured format with named fields. It can be
converted into bytes for Kafka and parsed by Spark or the Python
consumer.

### Q8. Why use a producer rate limit?

It controls the replay speed and prevents the producer from publishing
the entire sample as quickly as the machine can read it.

### Q9. What does `flush()` do?

It waits for outstanding asynchronous producer sends to complete before
the program exits.

## Spark and streaming

### Q10. What is Spark Structured Streaming?

It is Spark's structured API for processing continuously arriving data
using DataFrame and SQL-style operations.

### Q11. Why define an explicit schema?

The schema tells Spark the expected names and types of fields in each
JSON message. It avoids depending on schema inference for each message.

### Q12. What does `from_json` do?

It parses a JSON string column into a structured column according to the
supplied schema.

### Q13. How are anomalies detected?

The code filters records using `rt > THRESHOLD`. The threshold defaults
to 500 ms and can be supplied as a command-line argument.

### Q14. What is a micro-batch?

It is a small group of records processed together during a streaming
trigger.

### Q15. What is a checkpoint?

A checkpoint stores streaming progress and related recovery information
so a query can resume from committed progress after a restart.

### Q16. Why are there three streaming queries?

One prints anomalies for testing, one archives all parsed records to
HDFS, and one publishes anomalies to a second Kafka topic for the HBase
bridge.

### Q17. Why not write directly to HBase from Spark using `foreachBatch`?

The selected Spark version is 2.2, while `foreachBatch` was introduced
in Spark 2.4. The project therefore uses a second Kafka topic and a
separate Python consumer as the bridge.

## HDFS and HBase

### Q18. What is HDFS?

HDFS is Hadoop's distributed file system. It stores the project's raw
Parquet archive and batch metrics.

### Q19. Why Parquet?

Parquet is columnar, compressed, and schema-aware. It is suitable for
analytical reads and aggregations.

### Q20. Why do you need HBase if you already have HDFS?

HDFS is designed for large sequential file operations. HBase supports
low-latency row access, which is more suitable for serving dashboard
data.

### Q21. What is an HBase row key?

It uniquely identifies a row. The anomaly table uses `traceid#rpcid`,
while the service-metrics table uses the downstream service identifier.

### Q22. What is a column family?

It is a group of related columns in an HBase table. The anomaly table
uses `metrics`, and the service metrics table uses `stats`.

### Q23. Why use HappyBase?

HappyBase is a Python client that communicates with HBase through its
Thrift service.

## Batch analytics

### Q24. What does `groupBy("dm")` do?

It groups records by downstream service so the job can calculate
separate metrics for each service.

### Q25. What is average latency?

It is the sum of response times divided by the number of calls.

### Q26. What is p95 latency?

It is the approximate 95th-percentile response time. Approximately 95%
of observed calls have response times at or below that value.

### Q27. Why calculate both average and p95?

The average summarizes typical latency but can hide a long tail. The p95
helps show slower experiences near the tail of the distribution.

### Q28. What is `slow_calls`?

It counts calls whose response time is strictly greater than 500 ms.

### Q29. Why exclude `(?)`?

It represents an unknown downstream service. It can remain in the raw
archive, but it cannot be attributed to a known service for per-service
statistics.

## API and dashboard

### Q30. What is FastAPI doing?

It exposes HBase data through HTTP endpoints that the dashboard can
call.

### Q31. What is a REST endpoint?

It is a URL and HTTP method through which a client can access an API
operation, such as `GET /api/anomalies/live`.

### Q32. Why does the dashboard not connect directly to HBase?

The API separates data access from presentation. The dashboard only
needs to make HTTP requests and display the returned JSON.

### Q33. What is Pandas used for?

It converts API results into DataFrames and supports grouping, sorting,
slicing, and reshaping the data for charts.

### Q34. What is Altair used for?

It creates the dashboard's visualizations, including the anomaly bar
chart, latency scatter plot, and average-versus-p95 comparison.

### Q35. How does automatic refresh work?

The `st.fragment` decorator uses `run_every=2` when the checkbox is
enabled, rerunning the live dashboard fragment every two seconds.

------------------------------------------------------------------------

# 13. Quick glossary

  ---------------------------------------------------------------------
  Term                               Short explanation
  ---------------------------------- ----------------------------------
  AIOps                              Applying data and analytics to IT
                                     operations

  RCA                                Root-cause analysis

  Microservice                       A small service that performs part
                                     of an application

  Trace                              A record of the calls belonging to
                                     one request

  RPC                                Remote procedure call

  Latency / `rt`                     Time taken for a call, in
                                     milliseconds here

  Producer                           Publishes records to Kafka

  Consumer                           Reads records from Kafka

  Broker                             Kafka server that receives and
                                     serves records

  Topic                              Named Kafka stream

  Serialization                      Converting an object into a
                                     transport format

  Deserialization                    Reconstructing an object from
                                     serialized data

  Streaming                          Processing records as they arrive

  Batch processing                   Processing stored data in groups

  Micro-batch                        Small group of records processed
                                     by a streaming trigger

  Checkpoint                         Saved progress used for stream
                                     recovery

  HDFS                               Hadoop Distributed File System

  Parquet                            Columnar analytical file format

  HBase                              Distributed wide-column NoSQL
                                     database

  Row key                            HBase row identifier

  Column family                      Group of related HBase columns

  Thrift                             Service interface used here by
                                     HappyBase to communicate with
                                     HBase

  DataFrame                          Distributed structured data
                                     abstraction in Spark; in Pandas,
                                     an in-memory table

  Aggregation                        Calculating summaries over groups
                                     of records

  Average                            Arithmetic mean

  p95                                95th-percentile value

  REST API                           HTTP interface for retrieving or
                                     manipulating resources

  Endpoint                           URL handled by an API operation

  Dashboard                          Visual interface for displaying
                                     metrics
  ---------------------------------------------------------------------

------------------------------------------------------------------------

# 14. Final mental model

Remember the responsibility of each file:

  ---------------------------------------------------------------------
  File                               One-sentence explanation
  ---------------------------------- ----------------------------------
  `producer.py`                      Reads and validates CSV records,
                                     then publishes them to Kafka.

  `streaming_job.py`                 Reads Kafka records with Spark,
                                     archives all records, and routes
                                     slow calls to an anomaly topic.

  `kafka_to_hbase.py`                Consumes anomaly messages and
                                     writes them into the HBase
                                     anomalies table.

  `batch_job.py`                     Reads the Parquet archive,
                                     calculates per-service historical
                                     metrics, and stores the results.

  `api.py`                           Exposes anomaly and service-metric
                                     data from HBase through FastAPI
                                     endpoints.

  `dashboard.py`                     Calls the API and displays live
                                     anomalies and batch analytics with
                                     Streamlit.
  ---------------------------------------------------------------------

The key distinction to remember for the defense is:

**Kafka transports events, Spark processes them, HDFS stores the
historical archive, HBase serves queryable results, FastAPI exposes
those results, and Streamlit displays them.**

------------------------------------------------------------------------

# 15. Live demo: terminals, commands, and what to check

This section describes how to run the complete pipeline using the
commands and configuration in the project README. The setup has two
phases:

1.  **One-time infrastructure and data preparation**
2.  **Live demo processes**, which run concurrently in separate
    terminals

The commands below distinguish between **Windows PowerShell** and the
**Linux shell inside `hadoop-master`**. Do not run Linux container
commands directly in PowerShell unless they are prefixed with
`docker exec`.

## 15.1 How many terminals do you need?

For the live part, prepare these five main terminals:

  Terminal   Where it runs                     Process
  ---------- --------------------------------- ----------------------------
  1          Windows PowerShell                FastAPI (`api.py`)
  2          Windows PowerShell                Streamlit (`dashboard.py`)
  3          `hadoop-master` container shell   Spark streaming job
  4          `hadoop-master` container shell   Kafka-to-HBase bridge
  5          `hadoop-master` container shell   CSV producer

You can open additional PowerShell windows for Docker setup, HDFS
checks, and HBase checks. After the producer finishes, its terminal can
be reused to run the batch job. Therefore, five concurrent process
terminals are sufficient for the main live demonstration, provided the
Docker infrastructure is already running.

## 15.2 Phase A --- Start the Docker infrastructure (Windows PowerShell)

Open PowerShell. Create the Docker network:

``` powershell
docker network create --driver bridge hadoop
```

Start the Hadoop master:

``` powershell
docker run -itd --net hadoop -p 50070:50070 -p 8088:8088 -p 7077:7077 -p 16010:16010 -p 9090:9090 -p 8000:8000 -p 8501:8501 --name hadoop-master --hostname hadoop-master liliasfaxi/spark-hadoop:hv-2.7.2
```

Start two Hadoop workers:

``` powershell
docker run -itd --net hadoop --name hadoop-slave1 --hostname hadoop-slave1 liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd --net hadoop --name hadoop-slave2 --hostname hadoop-slave2 liliasfaxi/spark-hadoop:hv-2.7.2
```

Start Zookeeper and Kafka:

``` powershell
docker run -d --name zookeeper --net hadoop -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper:7.4.0
docker run -d --name kafka --net hadoop -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:7.4.0
```

Create the Kafka topics:

``` powershell
docker exec kafka kafka-topics --create --topic microservices-logs --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic anomalies --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

These commands are the initial setup described in the README. If the
containers, network, or topics already exist, do not blindly recreate
them; check their status first:

``` powershell
docker ps -a
docker network ls
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

## 15.3 Phase B --- Initialize Hadoop, HBase, and Python libraries

Enter the master container:

``` powershell
docker exec -it hadoop-master bash
```

The prompt should now look similar to:

``` text
root@hadoop-master:~#
```

Run the following inside the container:

``` bash
./start-hadoop.sh
hdfs dfs -mkdir -p /telemetry/input /telemetry/raw /telemetry/metrics
apt-get update && apt-get install -y python3-pip
python3 -m pip install --upgrade "pip<21"
pip3 install kafka-python==2.0.2 happybase==1.2.0
start-hbase.sh
hbase thrift start -p 9090 > /root/thrift.log 2>&1 &
```

Create the HBase tables:

``` bash
hbase shell
```

At the HBase shell prompt, execute:

``` ruby
create 'anomalies', 'metrics'
create 'service_metrics', 'stats'
exit
```

The two tables are:

-   `anomalies`, with column family `metrics`.
-   `service_metrics`, with column family `stats`.

The Thrift service is needed because the Python processes use HappyBase
to access HBase.

## 15.4 Phase C --- Prepare the sample data

The README's sample-generation command is run on Windows PowerShell in
the directory containing the original `MSCallGraph_0.csv`:

``` powershell
Get-Content MSCallGraph_0.csv -TotalCount 300001 | Set-Content MS_CallGraph_sample.csv
```

Copy the sample into the master container:

``` powershell
docker cp MS_CallGraph_sample.csv hadoop-master:/root/
```

Then, inside the master container, copy it into HDFS:

``` bash
hdfs dfs -put /root/MS_CallGraph_sample.csv /telemetry/input/
```

The producer reads the container copy at
`/root/MS_CallGraph_sample.csv`. The streaming archive is written
separately to `/telemetry/raw`.

## 15.5 Phase D --- Start the five main process terminals

### Terminal 1 --- FastAPI on Windows

Open a new PowerShell window and go to the Windows project folder:

``` powershell
cd C:\Users\basse\aiops
python -m uvicorn api:app --host 127.0.0.1 --port 8010 --ws none
```

This starts the API on `http://127.0.0.1:8010`.

Useful check in a browser:

``` text
http://127.0.0.1:8010/api/health
```

FastAPI's interactive documentation is normally available at:

``` text
http://127.0.0.1:8010/docs
```

### Terminal 2 --- Streamlit dashboard on Windows

Open another PowerShell window:

``` powershell
cd C:\Users\basse\aiops
python -m streamlit run dashboard.py --server.port 8502
```

Open the dashboard at:

``` text
http://localhost:8502
```

The dashboard calls the API, so keep Terminal 1 running.

### Terminal 3 --- Spark Structured Streaming inside Docker

Open another PowerShell window and enter the master container:

``` powershell
docker exec -it hadoop-master bash
```

Inside the container, run:

``` bash
export PYSPARK_PYTHON=python3
spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 /root/streaming_job.py 200
```

The `200` argument selects the lower demo threshold. To use the
project's normal rule, replace it with `500`.

Keep this process running. It starts three streaming queries: console
output, raw Parquet archive, and anomalies-to-Kafka.

### Terminal 4 --- Kafka-to-HBase bridge inside Docker

Open another PowerShell window:

``` powershell
docker exec -it hadoop-master bash
```

Inside the container:

``` bash
python3 /root/kafka_to_hbase.py
```

Keep it running. It consumes the `anomalies` topic and writes anomaly
rows to HBase.

### Terminal 5 --- Producer inside Docker

Open another PowerShell window:

``` powershell
docker exec -it hadoop-master bash
```

Inside the container:

``` bash
python3 /root/producer.py --file /root/MS_CallGraph_sample.csv --rate 800
```

This starts replaying the sample at the configured approximate rate. The
producer prints progress every 10,000 queued messages and prints a final
`DONE` line when it finishes.

## 15.6 What should you observe during the demo?

  -----------------------------------------------------------------------
  Where to look                       Expected observation
  ----------------------------------- -----------------------------------
  Producer terminal                   `sent=... skipped=...` progress
                                      messages, followed by a final
                                      `DONE` line

  Spark terminal                      Streaming query activity and
                                      anomaly rows printed by the console
                                      query

  Bridge terminal                     `written to HBase: ...` counter
                                      increasing as anomaly messages are
                                      consumed

  Dashboard                           Live anomaly count and charts
                                      update; batch service metrics
                                      appear after the batch job has run

  API health endpoint                 JSON response showing status and
                                      HBase table names

  HDFS                                Parquet files appear under
                                      `/telemetry/raw` as the archive
                                      query commits batches
  -----------------------------------------------------------------------

The dashboard's live fragment refreshes every two seconds when its
auto-refresh checkbox is enabled. The batch metrics do not get
recalculated every two seconds; they change when `batch_job.py` is run
again.

## 15.7 Check that the raw archive has caught up

The producer can finish before Spark has archived every message. The
README recommends waiting until the raw archive checkpoint reaches
approximately batch 47 for the 300,000-row sample.

Inside the master container, run:

``` bash
hdfs dfs -ls /telemetry/checkpoints/raw/commits
```

The README also provides this command to inspect the latest committed
batch number:

``` bash
hdfs dfs -ls /telemetry/checkpoints/raw/commits | awk '{print $NF}' | grep -o '[0-9]*$' | sort -n | tail -1
```

For the sample described in the README, the archive used 48
micro-batches, numbered 0 through 47. Wait for the archive to catch up
before running the batch job.

## 15.8 Run the batch job

Once the producer has finished and the raw archive has caught up, you
can reuse Terminal 5 after the producer exits, or open another container
shell:

``` powershell
docker exec -it hadoop-master bash
```

Then run:

``` bash
spark-submit --master local[2] --driver-memory 1g /root/batch_job.py
```

Expected output includes:

-   The number of raw rows in the archive.
-   The number of services profiled.
-   The number of rows written to HBase.
-   The top ten services by slow-call count.

Refresh the dashboard. The batch latency chart should now display the
service metrics written by this job.

## 15.9 Useful verification commands

Run these from Windows PowerShell:

``` powershell
docker ps
```

Check Kafka topics:

``` powershell
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

Open a shell in the master:

``` powershell
docker exec -it hadoop-master bash
```

Inside the master container, inspect HDFS:

``` bash
hdfs dfs -ls /telemetry
hdfs dfs -ls /telemetry/raw
hdfs dfs -ls /telemetry/metrics
```

Inspect HBase tables from the HBase shell:

``` bash
hbase shell
```

Then:

``` ruby
list
count 'anomalies'
count 'service_metrics'
```

Exit the HBase shell with:

``` ruby
exit
```

## 15.10 Demo sequence to present to the professor

A clear live demonstration can follow this order:

1.  Show `docker ps` to prove that the required containers are running.
2.  Show the HDFS web interface at `http://localhost:50070`.
3.  Show the producer terminal and its `sent` counter.
4.  Show Spark detecting and printing anomaly records.
5.  Show the bridge terminal writing anomalies to HBase.
6.  Show the dashboard updating its live anomaly count, charts, and
    table.
7.  Show the raw archive in HDFS.
8.  Run the batch job after the archive catches up.
9.  Refresh the dashboard and show the historical service-latency
    comparison.
10. Optionally demonstrate checkpoint recovery by stopping and
    restarting the streaming query with the same checkpoint locations.

## 15.11 Important operational notes

-   Keep the API, dashboard, Spark streaming query, and bridge running
    while the live demo is in progress.
-   Start the Spark streaming query and bridge before starting the
    producer so consumers are ready when records begin arriving.
-   The API and dashboard run on Windows; the producer, Spark jobs, and
    Kafka-to-HBase bridge run inside `hadoop-master`.
-   The API reaches HBase through the host's published Thrift port
    `9090`; inside the master container the bridge and batch job use
    `localhost:9090`.
-   The dashboard depends on the API, and the API depends on HBase.
-   Run the batch job only after the raw archive has caught up;
    otherwise its metrics describe incomplete input.
-   The commands in this section follow the supplied README. They are
    not a claim that every environment has already been initialized or
    that every command is safe to repeat on an existing deployment.
