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
