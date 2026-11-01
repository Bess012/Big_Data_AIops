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
