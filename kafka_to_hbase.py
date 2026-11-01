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
