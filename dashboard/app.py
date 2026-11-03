"""
SparkLens — Streamlit Dashboard
Spark cost & performance observatory: understand your job waste at a glance.
"""
from __future__ import annotations

import os
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SparkLens",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sparklens:sparklens123@postgres:5432/sparklens",
)

# ─── Styling ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Tighten top padding */
.main .block-container { padding-top: 1.2rem; }

/* KPI card labels */
[data-testid="stMetricLabel"]  { font-size: 0.78rem; color: #555; }
[data-testid="stMetricValue"]  { font-size: 1.6rem; font-weight: 700; }
[data-testid="stMetricDelta"]  { font-size: 0.72rem; }

/* Severity badges */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.04em;
}
.badge-CRITICAL { background:#c62828; color:#fff; }
.badge-HIGH     { background:#e65100; color:#fff; }
.badge-MEDIUM   { background:#f57f17; color:#fff; }
.badge-LOW      { background:#2e7d32; color:#fff; }

/* Suggestion card */
.sugg-card {
    border-left: 4px solid #ccc;
    border-radius: 6px;
    padding: 12px 16px;
    margin-bottom: 10px;
    background: #fafafa;
}

/* Horizontal rule with label */
.section-label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #999;
    margin: 0.4rem 0;
}

/* Diagnosis chip */
.diag-chip {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 600;
    background: #e3f2fd;
    color: #1565c0;
    margin-top: 4px;
}
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────

SEV_BORDER = {"CRITICAL": "#c62828", "HIGH": "#e65100", "MEDIUM": "#f57f17", "LOW": "#2e7d32"}
SEV_BG     = {"CRITICAL": "#fdecea", "HIGH": "#fff3e0", "MEDIUM": "#fffde7", "LOW": "#e8f5e9"}

WASTE_COLOR = {
    "critical": "#c62728",
    "high":     "#e65100",
    "medium":   "#f9a825",
    "ok":       "#2e7d32",
}

RULE_LABEL = {
    "HIGH_SHUFFLE":    "Excessive Shuffle",
    "MEMORY_SPILL":    "Memory Spill",
    "DATA_SKEW":       "Data Skew",
    "SMALL_FILES":     "Small Files / Many Partitions",
    "LOW_PARALLELISM": "Low CPU Utilisation",
}

RULE_ICON = {
    "HIGH_SHUFFLE":    "↔",
    "MEMORY_SPILL":    "💧",
    "DATA_SKEW":       "⚖",
    "SMALL_FILES":     "📄",
    "LOW_PARALLELISM": "⏳",
}

# One-line diagnosis per rule
RULE_DIAGNOSIS = {
    "HIGH_SHUFFLE":    "Large amount of data moved across the network during shuffle",
    "MEMORY_SPILL":    "Executor ran out of memory and wrote intermediate data to disk",
    "DATA_SKEW":       "One or more tasks are far slower than the rest — uneven key distribution",
    "SMALL_FILES":     "Too many tiny partitions create scheduling overhead with little actual work per task",
    "LOW_PARALLELISM": "CPUs are underutilised — not enough parallel tasks to keep the cluster busy",
}

# Copy-paste code fixes per rule
CODE_FIXES = {
    "HIGH_SHUFFLE": """\
# ── Fix 1: Enable broadcast join for small dimension tables ──────────────────
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")
# or explicitly in the query:
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# ── Fix 2: Let AQE choose the join strategy at runtime ───────────────────────
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
""",

    "MEMORY_SPILL": """\
# ── Fix 1: Increase executor memory ─────────────────────────────────────────
SparkSession.builder \\
    .config("spark.executor.memory", "4g") \\
    .config("spark.executor.memoryOverhead", "512m")

# ── Fix 2: Increase shuffle partition count (smaller per-partition size) ─────
spark.conf.set("spark.sql.shuffle.partitions", "400")

# ── Fix 3: Push filters and projections earlier ──────────────────────────────
df = df.filter(col("date") == "2024-01-01").select("id", "amount")  # before join
""",

    "DATA_SKEW": """\
# ── Fix 1: Enable AQE skew-join handling (Spark 3+, zero code change) ────────
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# ── Fix 2: Salt the hot key manually ────────────────────────────────────────
from pyspark.sql.functions import rand, concat, lit, cast
SALT = 10
df = df.withColumn("salt", (rand() * SALT).cast("int")) \\
       .withColumn("salted_key", concat("join_key", lit("_"), "salt"))
# replicate the small side by SALT
small_df = small_df.withColumn("salt", explode(array([lit(i) for i in range(SALT)]))) \\
                   .withColumn("salted_key", concat("join_key", lit("_"), "salt"))
""",

    "SMALL_FILES": """\
# ── Fix 1: Collapse small partitions without re-shuffle ─────────────────────
df = df.coalesce(20)           # fewer, larger partitions

# ── Fix 2: Raise the per-partition target size at read time ─────────────────
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB

# ── Fix 3: Compact files at source (Delta Lake) ──────────────────────────────
spark.sql("OPTIMIZE my_schema.my_table")
spark.sql("OPTIMIZE my_schema.my_table WHERE date >= '2024-01-01'")
""",

    "LOW_PARALLELISM": """\
# ── Fix 1: Match parallelism to available cores ──────────────────────────────
cores = spark.sparkContext.defaultParallelism
spark.conf.set("spark.default.parallelism", str(cores * 3))
spark.conf.set("spark.sql.shuffle.partitions", str(cores * 3))

# ── Fix 2: Repartition input data ────────────────────────────────────────────
df = df.repartition(cores * 3)

# ── Fix 3: Check for GC pressure ────────────────────────────────────────────
# Add to spark.executor.extraJavaOptions:
# -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
""",
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def fmt_job_name(raw: str) -> str:
    """benchmark_data_skew → Data Skew  |  my_etl_pipeline_v2 → My Etl Pipeline V2"""
    for prefix in ("benchmark_", "spark_job_", "job_", "spark_"):
        if raw.lower().startswith(prefix):
            raw = raw[len(prefix):]
    return raw.replace("_", " ").replace("-", " ").title()


def fmt_bytes(b, zero_label="0 B") -> str:
    if b is None or b == 0:
        return zero_label
    b = float(b)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def fmt_ms(ms) -> str:
    if ms is None or ms == 0:
        return "0 ms"
    ms = int(ms)
    if ms < 1_000:
        return f"{ms} ms"
    if ms < 60_000:
        return f"{ms / 1000:.1f} s"
    return f"{ms / 60_000:.1f} min"


def waste_tier(score: float) -> str:
    if score >= 60:
        return "critical"
    if score >= 30:
        return "high"
    if score >= 10:
        return "medium"
    return "ok"


def waste_label(score: float) -> str:
    return waste_tier(score).upper().replace("ok", "OK")


# ─── DB ───────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


@st.cache_data(ttl=20)
def load_jobs() -> pd.DataFrame:
    sql = text("""
        SELECT
            j.id,
            j.app_name,
            j.duration_ms,
            j.status,
            j.collected_at,
            COALESCE(m.total_shuffle_read_bytes,  0) AS shuffle_read,
            COALESCE(m.total_shuffle_write_bytes, 0) AS shuffle_write,
            COALESCE(m.total_spill_bytes,         0) AS spill_bytes,
            COALESCE(m.total_input_bytes,         0) AS input_bytes,
            COALESCE(m.total_executor_time_ms,    0) AS executor_ms,
            COALESCE(m.estimated_cost_usd,        0) AS cost_usd,
            COALESCE(m.waste_score,               0) AS waste_score,
            COALESCE(m.num_stages,                0) AS num_stages,
            COALESCE(m.num_tasks,                 0) AS num_tasks
        FROM jobs j
        LEFT JOIN job_metrics m ON m.job_id = j.id
        ORDER BY waste_score DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=20)
def load_stages(job_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT stage_id, stage_name, num_tasks, duration_ms,
               input_bytes, shuffle_read_bytes, shuffle_write_bytes,
               memory_spill_bytes, disk_spill_bytes, executor_run_time_ms
        FROM stages
        WHERE job_id = :job_id
        ORDER BY stage_id
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"job_id": job_id})


@st.cache_data(ttl=20)
def load_top_suggestion_per_job() -> pd.DataFrame:
    """Return the single highest-severity suggestion for every job (for the overview table)."""
    sql = text("""
        SELECT DISTINCT ON (job_id)
            job_id, rule_name, severity, message
        FROM suggestions
        ORDER BY job_id,
            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH'     THEN 2
                WHEN 'MEDIUM'   THEN 3
                ELSE 4
            END
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=20)
def load_suggestions(job_id: int) -> pd.DataFrame:
    sql = text("""
        SELECT rule_name, severity, message, detail
        FROM suggestions
        WHERE job_id = :job_id
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH'     THEN 2
                WHEN 'MEDIUM'   THEN 3
                ELSE 4
            END
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"job_id": job_id})


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("# 🔍 SparkLens")
    st.caption("Spark Cost & Performance Observatory")
    st.divider()

    auto_refresh = st.toggle("Auto-refresh (30 s)", value=True)
    if st.button("↺  Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("**Waste Score Guide**")
    for tier, color, label, rng in [
        ("ok",       "#2e7d32", "OK",       "0–9"),
        ("medium",   "#f9a825", "MEDIUM",   "10–29"),
        ("high",     "#e65100", "HIGH",     "30–59"),
        ("critical", "#c62728", "CRITICAL", "60–100"),
    ]:
        st.markdown(
            f"<span style='display:inline-block;width:12px;height:12px;"
            f"border-radius:3px;background:{color};margin-right:6px'></span>"
            f"<small><b>{label}</b> — {rng}</small>",
            unsafe_allow_html=True,
        )

    st.divider()
    st.markdown("**Cluster Links**")
    st.markdown("- [Spark Master UI :8080](http://localhost:8080)")
    st.markdown("- [History Server :18080](http://localhost:18080)")
    st.divider()
    st.caption("Metrics collected every 15 s from Spark event logs.")


# ─── Load data ────────────────────────────────────────────────────────────────

try:
    df = load_jobs()
except Exception as exc:
    st.error(f"**Database connection failed**: {exc}")
    st.info("Make sure `docker compose up` is running and postgres is healthy.")
    st.stop()

if df.empty:
    st.info(
        "**No jobs profiled yet.**  \n"
        "The benchmark container is submitting jobs. "
        "This page will auto-refresh every 10 seconds."
    )
    if auto_refresh:
        time.sleep(10)
        st.cache_data.clear()
        st.rerun()
    st.stop()


# ─── Header ───────────────────────────────────────────────────────────────────

st.markdown("## 🔍 SparkLens — Spark Job Waste Analysis")
st.caption(
    "Profiled jobs are ranked by **Waste Score** — a 0–100 measure of how much "
    "compute, memory, and network each job is wasting. Lower is better. "
    "Suggestions show you exactly what to change and how."
)

# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab_overview, tab_jobs, tab_howto = st.tabs(
    ["Overview", "Job Analysis", "How It Works"]
)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

with tab_overview:

    # ── KPI row ───────────────────────────────────────────────────────────────
    total_shuffle = df["shuffle_read"].sum() + df["shuffle_write"].sum()
    total_spill   = df["spill_bytes"].sum()
    total_cost    = float(df["cost_usd"].sum())
    avg_waste     = float(df["waste_score"].mean())
    jobs_with_issues = int((df["waste_score"] > 9).sum())

    k1, k2, k3, k4, k5 = st.columns(5)

    k1.metric(
        "Jobs Profiled",
        len(df),
        help="Total number of Spark applications analysed.",
    )
    k2.metric(
        "Jobs With Issues",
        f"{jobs_with_issues} / {len(df)}",
        help="Jobs with a Waste Score above 9 (MEDIUM or higher).",
    )
    k3.metric(
        "Total Shuffle",
        fmt_bytes(total_shuffle),
        help="Total data moved across the network for shuffles (read + write). "
             "High shuffle = slow jobs and wasted bandwidth.",
    )
    k4.metric(
        "Total Spill",
        fmt_bytes(total_spill, zero_label="None ✓"),
        help="Data written to disk because executors ran out of memory. "
             "Any spill is a performance red flag.",
    )
    k5.metric(
        "Avg Waste Score",
        f"{avg_waste:.1f} / 100",
        help="Average waste score across all jobs. "
             "0 = perfectly efficient. 100 = maximum waste.",
    )

    st.divider()

    # ── Left: Waste ranking  |  Right: Comparison scatter ─────────────────────
    col_rank, col_scatter = st.columns([1, 1], gap="large")

    with col_rank:
        st.markdown("#### Waste Score Ranking")
        st.caption("Red = high waste, green = efficient. Fix the red jobs first.")

        rank_df = df[["app_name", "waste_score"]].copy()
        rank_df["tier"]  = rank_df["waste_score"].apply(waste_tier)
        rank_df["color"] = rank_df["tier"].map(WASTE_COLOR)
        rank_df["label"] = rank_df["waste_score"].apply(lambda s: f"{s:.1f}")
        rank_df["Job"]   = rank_df["app_name"].apply(fmt_job_name)

        fig_rank = go.Figure(go.Bar(
            x=rank_df["waste_score"],
            y=rank_df["Job"],
            orientation="h",
            marker_color=rank_df["color"],
            text=rank_df["label"],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Waste Score: %{x:.1f}<extra></extra>",
        ))
        fig_rank.update_layout(
            height=max(180, len(df) * 70),
            xaxis=dict(title="Waste Score  (0 = clean, 100 = worst)", range=[0, 115]),
            yaxis=dict(title="", autorange="reversed"),
            margin=dict(l=0, r=30, t=10, b=30),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_rank, use_container_width=True)

    with col_scatter:
        st.markdown("#### Duration vs Shuffle")
        st.caption(
            "Each bubble is one job. "
            "Bubble **size** = spill bytes. "
            "Ideal job: bottom-left, small bubble."
        )

        scatter_df = df.copy()
        scatter_df["Job"]          = scatter_df["app_name"].apply(fmt_job_name)
        scatter_df["Shuffle (MB)"] = (scatter_df["shuffle_read"] + scatter_df["shuffle_write"]) / 1_048_576
        scatter_df["Spill (MB)"]   = scatter_df["spill_bytes"] / 1_048_576
        scatter_df["Duration (s)"] = scatter_df["duration_ms"] / 1_000

        # Give zero-spill jobs a minimum visible size
        scatter_df["bubble_size"] = scatter_df["Spill (MB)"].clip(lower=5)

        fig_sc = px.scatter(
            scatter_df,
            x="Duration (s)",
            y="Shuffle (MB)",
            size="bubble_size",
            color="waste_score",
            hover_name="Job",
            hover_data={
                "Duration (s)":   ":.1f",
                "Shuffle (MB)":   ":.1f",
                "Spill (MB)":     ":.1f",
                "waste_score":    ":.1f",
                "bubble_size":    False,
            },
            color_continuous_scale="RdYlGn_r",
            range_color=[0, 100],
            labels={"waste_score": "Waste Score"},
            size_max=50,
        )
        fig_sc.update_layout(
            height=max(180, len(df) * 70),
            margin=dict(l=0, r=0, t=10, b=30),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(title="Waste Score", thickness=12),
        )
        st.plotly_chart(fig_sc, use_container_width=True)

    st.divider()

    # ── Summary table ──────────────────────────────────────────────────────────
    st.markdown("#### All Jobs — Summary")

    top_sugg = load_top_suggestion_per_job()  # {job_id → (rule_name, severity, message)}

    tbl = df.copy()
    tbl["Job"]         = tbl["app_name"].apply(fmt_job_name)
    tbl["Waste Score"] = tbl["waste_score"].apply(lambda s: f"{s:.1f}")
    tbl["Duration"]    = tbl["duration_ms"].apply(fmt_ms)
    tbl["Shuffle"]     = (tbl["shuffle_read"] + tbl["shuffle_write"]).apply(fmt_bytes)
    tbl["Spill"]       = tbl["spill_bytes"].apply(lambda b: fmt_bytes(b, "None ✓"))
    tbl["Est. Cost"]   = tbl["cost_usd"].apply(lambda c: f"${float(c):.5f}")
    tbl["Stages"]      = tbl["num_stages"]
    tbl["Tasks"]       = tbl["num_tasks"]

    # Merge top suggestion
    if not top_sugg.empty:
        top_sugg["Top Issue"] = (
            top_sugg["severity"] + " — "
            + top_sugg["rule_name"].map(RULE_LABEL).fillna(top_sugg["rule_name"])
        )
        tbl = tbl.merge(
            top_sugg[["job_id", "Top Issue"]].rename(columns={"job_id": "id"}),
            on="id", how="left",
        )
        tbl["Top Issue"] = tbl["Top Issue"].fillna("—")
    else:
        tbl["Top Issue"] = "—"

    st.dataframe(
        tbl[["Job", "Waste Score", "Top Issue", "Duration", "Shuffle", "Spill", "Est. Cost", "Stages", "Tasks"]],
        use_container_width=True,
        hide_index=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — JOB ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

with tab_jobs:

    # ── Job selector ──────────────────────────────────────────────────────────
    job_options = {
        f"{fmt_job_name(row['app_name'])}  —  score {row['waste_score']:.1f}": int(row["id"])
        for _, row in df.iterrows()
    }
    selected_label = st.selectbox(
        "Select a job to inspect:",
        list(job_options.keys()),
        index=0,
    )
    selected_id  = job_options[selected_label]
    job_row      = df[df["id"] == selected_id].iloc[0]
    ws           = float(job_row["waste_score"])
    tier         = waste_tier(ws)
    tier_color   = WASTE_COLOR[tier]

    # ── Job header ────────────────────────────────────────────────────────────
    sugg_df   = load_suggestions(selected_id)
    stages_df = load_stages(selected_id)

    top_rule  = sugg_df.iloc[0]["rule_name"] if not sugg_df.empty else None
    diagnosis = RULE_DIAGNOSIS.get(top_rule, "No issues detected for this job.")
    diag_icon = RULE_ICON.get(top_rule, "✓")

    st.markdown(
        f"<h4 style='margin-bottom:4px'>{fmt_job_name(job_row['app_name'])}"
        f"<span style='font-size:0.7rem;font-weight:400;color:#999;margin-left:10px'>"
        f"({job_row['app_name']})</span></h4>"
        f"<span class='diag-chip'>{diag_icon} Primary issue: {diagnosis}</span>",
        unsafe_allow_html=True,
    )

    # ── Three-column overview: gauge | key metrics | top suggestion ───────────
    col_gauge, col_metrics, col_top = st.columns([1, 1, 1.6], gap="large")

    with col_gauge:
        st.markdown("<p class='section-label'>Waste Score</p>", unsafe_allow_html=True)
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=ws,
            number={"suffix": " / 100", "font": {"size": 22}},
            gauge={
                "axis":  {"range": [0, 100], "tickfont": {"size": 10}},
                "bar":   {"color": tier_color, "thickness": 0.28},
                "bgcolor": "white",
                "borderwidth": 0,
                "steps": [
                    {"range": [0,  10], "color": "#e8f5e9"},
                    {"range": [10, 30], "color": "#fffde7"},
                    {"range": [30, 60], "color": "#fff3e0"},
                    {"range": [60, 100], "color": "#fdecea"},
                ],
                "threshold": {
                    "line": {"color": tier_color, "width": 3},
                    "thickness": 0.8,
                    "value": ws,
                },
            },
        ))
        fig_gauge.update_layout(
            height=200,
            margin=dict(l=20, r=20, t=30, b=10),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

        sev_label = waste_label(ws)
        st.markdown(
            f"<center><span class='badge badge-{sev_label if sev_label != 'OK' else 'LOW'}'>"
            f"  {sev_label}  </span></center>",
            unsafe_allow_html=True,
        )

    with col_metrics:
        st.markdown("<p class='section-label'>Key Metrics</p>", unsafe_allow_html=True)
        st.metric("Duration",        fmt_ms(job_row["duration_ms"]))
        st.metric("Shuffle",         fmt_bytes(job_row["shuffle_read"] + job_row["shuffle_write"]))
        st.metric("Spill to Disk",   fmt_bytes(job_row["spill_bytes"], "None ✓"))
        st.metric("Est. Compute Cost", f"${float(job_row['cost_usd']):.5f}")
        st.metric("Stages / Tasks",  f"{int(job_row['num_stages'])} stages, {int(job_row['num_tasks'])} tasks")

    with col_top:
        st.markdown("<p class='section-label'>Issues Found</p>", unsafe_allow_html=True)
        if sugg_df.empty:
            st.success("No issues detected — this job looks efficient!")
        else:
            for _, sg in sugg_df.iterrows():
                sev   = sg["severity"]
                rule  = sg["rule_name"]
                bdr   = SEV_BORDER.get(sev, "#888")
                bg    = SEV_BG.get(sev, "#fafafa")
                icon  = RULE_ICON.get(rule, "•")
                label = RULE_LABEL.get(rule, rule)
                st.markdown(
                    f"""<div class='sugg-card' style='border-left-color:{bdr};background:{bg}'>
                        <span class='badge badge-{sev}'>{sev}</span>
                        &nbsp;<strong>{icon} {label}</strong><br/>
                        <small style='color:#555;line-height:1.5'>{sg['message']}</small>
                    </div>""",
                    unsafe_allow_html=True,
                )

    st.divider()

    # ── Stage breakdown ────────────────────────────────────────────────────────
    if not stages_df.empty:
        st.markdown("#### Stage Breakdown")
        st.caption(
            "Each row is one Spark stage. "
            "Shuffle = data moved over the network. "
            "Spill = data written to disk due to memory pressure."
        )

        left_chart, right_chart = st.columns(2, gap="medium")

        with left_chart:
            # Duration bar
            sd = stages_df.copy()
            sd["Stage"] = "S" + sd["stage_id"].astype(str) + ": " + sd["stage_name"].str[:25]
            sd["Duration (s)"] = sd["duration_ms"] / 1000
            fig_dur = px.bar(
                sd, x="Duration (s)", y="Stage",
                orientation="h",
                title="Stage Duration",
                color="Duration (s)",
                color_continuous_scale="Blues",
            )
            fig_dur.update_layout(
                height=max(200, len(sd) * 50 + 60),
                margin=dict(l=0, r=0, t=40, b=10),
                showlegend=False,
                coloraxis_showscale=False,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                yaxis={"autorange": "reversed"},
            )
            st.plotly_chart(fig_dur, use_container_width=True)

        with right_chart:
            # Shuffle + spill chart
            cd = stages_df.copy()
            cd["Stage"]        = "S" + cd["stage_id"].astype(str) + ": " + cd["stage_name"].str[:20]
            cd["Shuffle (MB)"] = (cd["shuffle_read_bytes"] + cd["shuffle_write_bytes"]) / 1_048_576
            cd["Spill (MB)"]   = (cd["memory_spill_bytes"] + cd["disk_spill_bytes"]) / 1_048_576

            fig_sh = px.bar(
                cd, x="Stage",
                y=["Shuffle (MB)", "Spill (MB)"],
                barmode="group",
                title="Shuffle & Spill per Stage",
                color_discrete_map={"Shuffle (MB)": "#1565c0", "Spill (MB)": "#c62728"},
            )
            fig_sh.update_layout(
                height=max(200, len(cd) * 50 + 60),
                margin=dict(l=0, r=0, t=40, b=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", y=1.12),
            )
            st.plotly_chart(fig_sh, use_container_width=True)

        # Stage detail table
        with st.expander("Stage detail table", expanded=False):
            tbl_s = stages_df.copy()
            tbl_s["Stage Name"]    = tbl_s["stage_name"]
            tbl_s["Tasks"]         = tbl_s["num_tasks"]
            tbl_s["Duration"]      = tbl_s["duration_ms"].apply(fmt_ms)
            tbl_s["Input"]         = tbl_s["input_bytes"].apply(fmt_bytes)
            tbl_s["Shuffle Read"]  = tbl_s["shuffle_read_bytes"].apply(fmt_bytes)
            tbl_s["Shuffle Write"] = tbl_s["shuffle_write_bytes"].apply(fmt_bytes)
            tbl_s["Mem Spill"]     = tbl_s["memory_spill_bytes"].apply(fmt_bytes)
            tbl_s["Disk Spill"]    = tbl_s["disk_spill_bytes"].apply(fmt_bytes)
            tbl_s["Executor Time"] = tbl_s["executor_run_time_ms"].apply(fmt_ms)
            st.dataframe(
                tbl_s[["Stage Name", "Tasks", "Duration", "Input",
                        "Shuffle Read", "Shuffle Write", "Mem Spill",
                        "Disk Spill", "Executor Time"]],
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    # ── Fix suggestions with code ──────────────────────────────────────────────
    if not sugg_df.empty:
        st.markdown("#### How to Fix It")
        st.caption(
            "Each fix is ranked by severity. "
            "Apply the CRITICAL/HIGH fixes first — they have the biggest impact."
        )

        for _, sg in sugg_df.iterrows():
            rule  = sg["rule_name"]
            sev   = sg["severity"]
            bdr   = SEV_BORDER.get(sev, "#888")
            bg    = SEV_BG.get(sev, "#fafafa")
            icon  = RULE_ICON.get(rule, "•")
            label = RULE_LABEL.get(rule, rule)

            with st.expander(
                f"{icon}  {label}  —  {sev}", expanded=(sev in ("CRITICAL", "HIGH"))
            ):
                st.markdown(
                    f"<div style='background:{bg};border-left:4px solid {bdr};"
                    f"padding:10px 14px;border-radius:4px;margin-bottom:8px'>"
                    f"<b>What's happening:</b> {sg['detail']}"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                if rule in CODE_FIXES:
                    st.markdown("**Copy-paste fix:**")
                    st.code(CODE_FIXES[rule], language="python")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — HOW IT WORKS
# ═══════════════════════════════════════════════════════════════════════════════

with tab_howto:
    st.markdown("### How SparkLens Works")

    c1, c2 = st.columns(2, gap="large")

    with c1:
        st.markdown("""
#### What is the Waste Score?

The Waste Score (0–100) is a single number that summarises how inefficiently
a Spark job used its resources. **Lower is better.**

It is computed from four components:

| Component | Max Points | What it measures |
|-----------|-----------|-----------------|
| Shuffle bytes | 30 | Network data movement (read + write) |
| Spill bytes | 30 | Data written to disk from memory pressure |
| Wall-clock duration | 20 | Raw elapsed time |
| Task skew ratio | 20 | Max task time ÷ median task time |

```
waste_score =
  min(shuffle_GB × 5,    30)   # shuffle component
+ min(spill_GB   × 15,   30)   # spill component   (weighted heavier)
+ min(duration_min × 2,  20)   # duration component
+ min((max_task / median_task − 1) × 5,  20)  # skew component
```

Spill is weighted 3× more than shuffle because it directly degrades
performance by causing disk I/O in the hot path.

---

#### How is Cost Estimated?

```
cost_usd = (total_executor_CPU_ms / 3_600_000) × num_executors × $0.12
```

`$0.12 / vCPU-hour` approximates AWS m5.large spot pricing.
This is a benchmark estimate — not real cloud spend.
""")

    with c2:
        st.markdown("""
#### Analysing Your Own Spark Jobs

SparkLens only needs the **Spark event log file** — it never connects to your cluster.

**Step 1 — Enable event logging in your Spark app**

```python
# Add to your SparkSession builder or spark-defaults.conf
spark = SparkSession.builder \\
    .config("spark.eventLog.enabled", "true") \\
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \\
    .getOrCreate()
```

Or set in `spark-defaults.conf`:
```
spark.eventLog.enabled   true
spark.eventLog.dir       file:///tmp/spark-events
```

**Step 2 — Copy the completed log file into SparkLens**

```bash
# Find where the log landed (one file per app, named by app ID)
ls /tmp/spark-events/
# → app-20240601-120000-0001

# Copy it into the running collector's watched directory
docker cp /tmp/spark-events/app-20240601-120000-0001 \\
    sparklens-collector:/spark-events/
```

The collector will detect and process it within **15 seconds**.

**Step 3 — (Optional) Skip the benchmark, analyse only your jobs**

```bash
# Start only the analysis stack — no Spark cluster needed
docker compose up postgres collector dashboard

# Then copy event logs in as in Step 2
```

**Step 4 — Find your job in the dashboard**

Open **Job Analysis** → select your job from the dropdown.
The job name comes from `spark.app.name` in your Spark config.

---

> **Tip:** If your Spark cluster already writes event logs to a shared
> directory (HDFS, NFS, or a local path), you can mount that path
> directly as the `spark-events` Docker volume:
> ```yaml
> volumes:
>   spark-events:
>     driver: local
>     driver_opts:
>       type: none
>       o: bind
>       device: /your/existing/log/path
> ```

---

#### What Do the Rules Detect?

**HIGH_SHUFFLE** — triggered when total shuffle (read + write) exceeds **1 GB**.
Shuffle happens during joins, groupBy, and orderBy. It is the #1 cause of
slow Spark jobs. Fix: broadcast small tables, enable AQE.

---

**MEMORY_SPILL** — triggered when any spill (memory or disk) is detected.
Spill means executors ran out of heap space and wrote intermediate data to
the local disk, causing expensive I/O. Fix: increase executor memory or
raise shuffle partition count.

---

**DATA_SKEW** — triggered when the longest task in a stage is **>5×** the
median task time.
Skew means a few tasks handle far more data than others, so the stage
cannot finish until the stragglers complete. Fix: AQE skew-join, salting.

---

**SMALL_FILES** — triggered when average input per task is **< 1 MB** and
the stage has more than 50 tasks.
Reading thousands of tiny files wastes time on file-system metadata lookups
and task scheduling overhead. Fix: coalesce, compact files, raise maxPartitionBytes.

---

**LOW_PARALLELISM** — triggered when actual CPU time is **< 30%** of the
theoretical maximum for a stage.
Executors are mostly idle — often due to too few tasks, GC pressure, or
heavy serialisation. Fix: raise default.parallelism, profile GC.

---

#### Data Flow

```
Spark Cluster  →  Event Log (JSON)  →  Collector (Python)
                                             ↓
                                       PostgreSQL DB
                                             ↓
                                    Streamlit Dashboard
```

The collector polls the event log directory every **15 seconds**.
Logs are only processed after the application ends (`.inprogress` suffix
is removed by Spark when the app finishes).
""")


# ─── Auto-refresh ─────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(30)
    st.cache_data.clear()
    st.rerun()
