"""
Database layer (PostgreSQL via SQLAlchemy core).
"""
from __future__ import annotations

import os
from typing import List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from log_parser import JobLog
from analyzer import analyze, waste_score, cost_usd, Suggestion


def make_engine() -> Engine:
    url = os.environ["DATABASE_URL"]
    return create_engine(url, pool_pre_ping=True)


def get_processed_files(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT log_file FROM jobs")).fetchall()
    return {r[0] for r in rows}


def save_job(job: JobLog, engine: Engine) -> None:
    """
    Persist a completed JobLog and all derived data in a single transaction.
    Silently skips the job if app_id already exists (idempotent).
    """
    suggestions: List[Suggestion] = analyze(job)
    ws   = waste_score(job)
    cost = cost_usd(job)

    stages = job.stages
    total_shuffle_read  = sum(s.shuffle_read_bytes  for s in stages)
    total_shuffle_write = sum(s.shuffle_write_bytes for s in stages)
    total_spill         = sum(s.total_spill_bytes   for s in stages)
    total_input         = sum(s.input_bytes         for s in stages)
    total_exec_ms       = sum(s.executor_run_time_ms for s in stages)
    num_tasks           = sum(s.num_tasks            for s in stages)

    with engine.begin() as conn:
        # ── jobs ─────────────────────────────────────────────────────────────
        row = conn.execute(
            text("""
                INSERT INTO jobs (app_id, app_name, start_time, end_time,
                                  duration_ms, status, log_file)
                VALUES (:app_id, :app_name, :start_time, :end_time,
                        :duration_ms, :status, :log_file)
                ON CONFLICT (app_id) DO NOTHING
                RETURNING id
            """),
            {
                "app_id":      job.app_id,
                "app_name":    job.app_name,
                "start_time":  job.start_time,
                "end_time":    job.end_time,
                "duration_ms": job.duration_ms,
                "status":      job.status,
                "log_file":    job.log_file,
            },
        ).fetchone()

        if row is None:
            return   # already processed
        job_id = row[0]

        # ── stages ───────────────────────────────────────────────────────────
        for s in stages:
            conn.execute(
                text("""
                    INSERT INTO stages
                        (job_id, stage_id, stage_name, num_tasks, duration_ms,
                         input_bytes, output_bytes, shuffle_read_bytes,
                         shuffle_write_bytes, memory_spill_bytes,
                         disk_spill_bytes, executor_run_time_ms)
                    VALUES
                        (:job_id, :stage_id, :stage_name, :num_tasks, :duration_ms,
                         :input_bytes, :output_bytes, :shuffle_read_bytes,
                         :shuffle_write_bytes, :memory_spill_bytes,
                         :disk_spill_bytes, :executor_run_time_ms)
                """),
                {
                    "job_id":               job_id,
                    "stage_id":             s.stage_id,
                    "stage_name":           s.stage_name,
                    "num_tasks":            s.num_tasks,
                    "duration_ms":          s.duration_ms,
                    "input_bytes":          s.input_bytes,
                    "output_bytes":         s.output_bytes,
                    "shuffle_read_bytes":   s.shuffle_read_bytes,
                    "shuffle_write_bytes":  s.shuffle_write_bytes,
                    "memory_spill_bytes":   s.memory_spill_bytes,
                    "disk_spill_bytes":     s.disk_spill_bytes,
                    "executor_run_time_ms": s.executor_run_time_ms,
                },
            )

        # ── suggestions ──────────────────────────────────────────────────────
        for sg in suggestions:
            conn.execute(
                text("""
                    INSERT INTO suggestions (job_id, rule_name, severity, message, detail)
                    VALUES (:job_id, :rule_name, :severity, :message, :detail)
                """),
                {
                    "job_id":    job_id,
                    "rule_name": sg.rule_name,
                    "severity":  sg.severity,
                    "message":   sg.message,
                    "detail":    sg.detail,
                },
            )

        # ── job_metrics ──────────────────────────────────────────────────────
        conn.execute(
            text("""
                INSERT INTO job_metrics
                    (job_id, total_shuffle_read_bytes, total_shuffle_write_bytes,
                     total_spill_bytes, total_input_bytes, total_executor_time_ms,
                     estimated_cost_usd, waste_score, num_stages, num_tasks)
                VALUES
                    (:job_id, :total_shuffle_read_bytes, :total_shuffle_write_bytes,
                     :total_spill_bytes, :total_input_bytes, :total_executor_time_ms,
                     :estimated_cost_usd, :waste_score, :num_stages, :num_tasks)
            """),
            {
                "job_id":                    job_id,
                "total_shuffle_read_bytes":  total_shuffle_read,
                "total_shuffle_write_bytes": total_shuffle_write,
                "total_spill_bytes":         total_spill,
                "total_input_bytes":         total_input,
                "total_executor_time_ms":    total_exec_ms,
                "estimated_cost_usd":        cost,
                "waste_score":               ws,
                "num_stages":                len(stages),
                "num_tasks":                 num_tasks,
            },
        )
