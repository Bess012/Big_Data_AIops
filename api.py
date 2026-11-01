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
