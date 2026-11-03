# SparkLens — Architecture

## System Architecture

```mermaid
graph TB
    subgraph Docker Compose Network
        subgraph Spark Cluster
            SM[Spark Master<br/>bitnami/spark:3.5<br/>:8080 UI / :7077 RPC]
            SW[Spark Worker<br/>bitnami/spark:3.5<br/>2 cores / 2 GB]
            SH[Spark History Server<br/>bitnami/spark:3.5<br/>:18080]
            BM[Benchmark Runner<br/>custom image<br/>one-shot container]
        end

        subgraph Storage
            EV[(Event Log Volume<br/>shared mount)]
            PG[(PostgreSQL 15<br/>sparklens DB<br/>:5432)]
        end

        subgraph Observability
            CL[Collector Service<br/>Python 3.11<br/>polls every 15s]
            DB[Streamlit Dashboard<br/>:8501]
        end
    end

    BM -->|spark-submit| SM
    SM <-->|task scheduling| SW
    SM -->|writes JSON events| EV
    SW -->|writes JSON events| EV
    EV -->|reads history| SH
    EV -->|reads log files| CL
    CL -->|INSERT metrics + suggestions| PG
    PG -->|SELECT| DB
```

---

## Data Flow

```mermaid
sequenceDiagram
    participant BM as Benchmark Runner
    participant SM as Spark Master
    participant SW as Spark Worker
    participant EL as Event Log (volume)
    participant CL as Collector
    participant PG as PostgreSQL
    participant UI as Dashboard

    BM->>SM: spark-submit job_*.py
    SM->>SW: schedule stages & tasks
    SW-->>EL: SparkListenerTaskEnd (shuffle, spill, I/O)
    SM-->>EL: SparkListenerApplicationEnd
    Note over EL: file renamed<br/>.inprogress → complete

    loop Every 15 seconds
        CL->>EL: scan for new completed log files
        CL->>CL: parse JSON events line by line
        CL->>CL: compute waste score + cost estimate
        CL->>CL: run rule-based analyzer
        CL->>PG: INSERT jobs, stages, suggestions, metrics
    end

    UI->>PG: SELECT * FROM jobs JOIN metrics
    UI->>PG: SELECT * FROM suggestions WHERE job_id=?
    UI-->>UI: render ranked table + charts
```

---

## Component Breakdown

### 1. Spark Cluster (`bitnami/spark:3.5`)

| Role | Port | Purpose |
|------|------|---------|
| Master | 8080, 7077 | Web UI + cluster coordination |
| Worker | — | Task execution (2 cores, 2 GB) |
| History Server | 18080 | Browse completed job event logs |

**Key Spark config** (`spark-defaults.conf`):
```
spark.eventLog.enabled           true
spark.eventLog.dir               file:///opt/bitnami/spark/logs/events
spark.history.fs.logDirectory    file:///opt/bitnami/spark/logs/events
spark.sql.adaptive.enabled       false   # disabled for benchmarks to expose raw waste
```

---

### 2. Benchmark Jobs (PySpark)

Four synthetic jobs, each designed to expose a specific class of waste:

| Job | Waste Type | Technique |
|-----|-----------|-----------|
| `job_skew.py` | Data skew | 90% rows on one key, sort-merge join |
| `job_spill.py` | Memory spill | 512 MB executor limit + large sort |
| `job_small_files.py` | Too many partitions | 300 tiny partitions, 300 shuffle tasks |
| `job_bad_join.py` | Wrong join strategy | Broadcast threshold disabled, small dim table forced into sort-merge join |

---

### 3. Collector (Python 3.11)

```
collector/
├── main.py          # polling loop — watches event log dir every 15s
├── log_parser.py    # JSON event log parser → JobLog + StageInfo dataclasses
├── analyzer.py      # rule engine → Suggestion list + waste_score + cost estimate
└── db.py            # SQLAlchemy — persists to PostgreSQL
```

**Parsing pipeline:**
```
Event Log File (JSON lines)
  └── SparkListenerApplicationStart   → app_id, app_name, start_time
  └── SparkListenerStageCompleted     → stage duration, task count
  └── SparkListenerTaskEnd            → shuffle, spill, I/O, executor time
  └── SparkListenerApplicationEnd     → end_time → duration_ms
```

**Waste score formula (0–100):**
```
waste_score = min(shuffle_GB × 5, 30)     # shuffle component
            + min(spill_GB × 15, 30)      # spill component (heavily weighted)
            + min(duration_min × 2, 20)   # duration component
            + min((skew_ratio − 1) × 5, 20) # skew component
```

**Cost estimate:**
```
cost_usd = (total_executor_time_ms / 3_600_000) × num_executors × $0.12/vCPU-hr
```

**Rules engine:**

| Rule | Trigger | Severity |
|------|---------|---------|
| `HIGH_SHUFFLE` | shuffle > 1 GB | HIGH |
| `MEMORY_SPILL` | spill > 0 bytes | HIGH / CRITICAL |
| `DATA_SKEW` | max task / median task > 5× | HIGH |
| `SMALL_FILES` | avg input per task < 1 MB | MEDIUM |
| `LOW_PARALLELISM` | CPU utilization < 30% in a stage | MEDIUM |

---

### 4. PostgreSQL Schema

```mermaid
erDiagram
    jobs {
        serial id PK
        varchar app_id UK
        varchar app_name
        bigint  start_time
        bigint  end_time
        bigint  duration_ms
        varchar status
        varchar log_file
        timestamp collected_at
    }
    stages {
        serial  id PK
        int     job_id FK
        int     stage_id
        varchar name
        int     num_tasks
        bigint  duration_ms
        bigint  input_bytes
        bigint  shuffle_read_bytes
        bigint  shuffle_write_bytes
        bigint  memory_spill_bytes
        bigint  disk_spill_bytes
        bigint  executor_run_time_ms
    }
    suggestions {
        serial    id PK
        int       job_id FK
        varchar   rule_name
        varchar   severity
        text      message
        text      detail
        timestamp created_at
    }
    job_metrics {
        serial    id PK
        int       job_id FK
        bigint    total_shuffle_read_bytes
        bigint    total_shuffle_write_bytes
        bigint    total_spill_bytes
        bigint    total_executor_time_ms
        decimal   estimated_cost_usd
        decimal   waste_score
        int       num_stages
        int       num_tasks
    }

    jobs ||--o{ stages : "has"
    jobs ||--o{ suggestions : "has"
    jobs ||--|| job_metrics : "has"
```

---

### 5. Streamlit Dashboard

```
Page layout
├── Sidebar:  refresh rate, filters, about
└── Main
    ├── KPI row:  Jobs Profiled · Total Shuffle · Total Spill · Est. Cost · Avg Waste
    ├── Waste Score Chart:  horizontal bar chart (red→green), one bar per job
    ├── Jobs Table:  sortable, with inline waste score bars
    └── Job Detail (expander per job)
        ├── Stage Breakdown table
        ├── Shuffle + Spill bar chart (per stage)
        └── Suggestions list  (CRITICAL / HIGH / MEDIUM / LOW badges)
```

---

## Technology Stack

| Layer | Technology | Version | Why |
|-------|-----------|---------|-----|
| Compute | Apache Spark | 3.5 | Industry-standard distributed compute |
| Job language | PySpark | 3.5 | Python-native Spark API |
| Event logs | Spark JSON event log | built-in | Per-task granularity, no agent needed |
| Storage | PostgreSQL | 15 | Relational — easy joins across jobs/stages/suggestions |
| ORM / queries | SQLAlchemy | 2.x | Safe parameterized queries |
| Dashboard | Streamlit | latest | Fast Python-native dashboard |
| Charts | Plotly | latest | Interactive visualizations |
| Containerization | Docker Compose | v3.8 | One-command local setup |
| Base images | bitnami/spark:3.5, python:3.11-slim | — | Production-grade, minimal attack surface |

---

## Port Map

| Service | Port | URL |
|---------|------|-----|
| Spark Master UI | 8080 | http://localhost:8080 |
| Spark History Server | 18080 | http://localhost:18080 |
| PostgreSQL | 5432 | postgresql://localhost:5432/sparklens |
| SparkLens Dashboard | 8501 | http://localhost:8501 |
