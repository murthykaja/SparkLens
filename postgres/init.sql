-- SparkLens Database Schema

CREATE TABLE IF NOT EXISTS jobs (
    id           SERIAL PRIMARY KEY,
    app_id       VARCHAR(150) UNIQUE NOT NULL,
    app_name     VARCHAR(255) NOT NULL,
    start_time   BIGINT,
    end_time     BIGINT,
    duration_ms  BIGINT,
    status       VARCHAR(50) DEFAULT 'UNKNOWN',
    log_file     VARCHAR(1000),
    collected_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stages (
    id                   SERIAL PRIMARY KEY,
    job_id               INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    stage_id             INTEGER,
    stage_name           VARCHAR(500),
    num_tasks            INTEGER DEFAULT 0,
    duration_ms          BIGINT  DEFAULT 0,
    input_bytes          BIGINT  DEFAULT 0,
    output_bytes         BIGINT  DEFAULT 0,
    shuffle_read_bytes   BIGINT  DEFAULT 0,
    shuffle_write_bytes  BIGINT  DEFAULT 0,
    memory_spill_bytes   BIGINT  DEFAULT 0,
    disk_spill_bytes     BIGINT  DEFAULT 0,
    executor_run_time_ms BIGINT  DEFAULT 0
);

CREATE TABLE IF NOT EXISTS suggestions (
    id         SERIAL PRIMARY KEY,
    job_id     INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    rule_name  VARCHAR(100),
    severity   VARCHAR(20),   -- CRITICAL, HIGH, MEDIUM, LOW
    message    TEXT,
    detail     TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_metrics (
    id                        SERIAL PRIMARY KEY,
    job_id                    INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    total_shuffle_read_bytes  BIGINT  DEFAULT 0,
    total_shuffle_write_bytes BIGINT  DEFAULT 0,
    total_spill_bytes         BIGINT  DEFAULT 0,
    total_input_bytes         BIGINT  DEFAULT 0,
    total_executor_time_ms    BIGINT  DEFAULT 0,
    estimated_cost_usd        DECIMAL(10, 6) DEFAULT 0,
    waste_score               DECIMAL(6, 2)  DEFAULT 0,
    num_stages                INTEGER DEFAULT 0,
    num_tasks                 INTEGER DEFAULT 0
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_stages_job_id      ON stages(job_id);
CREATE INDEX IF NOT EXISTS idx_suggestions_job_id ON suggestions(job_id);
CREATE INDEX IF NOT EXISTS idx_metrics_job_id     ON job_metrics(job_id);
CREATE INDEX IF NOT EXISTS idx_metrics_waste      ON job_metrics(waste_score DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_collected     ON jobs(collected_at DESC);
