"""
Rule-Based Analyzer
===================
Examines parsed job/stage metrics and emits Suggestion objects,
a 0–100 waste_score, and a cost estimate in USD.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from log_parser import JobLog, StageInfo


# ── Cost constants ────────────────────────────────────────────────────────────
_RATE_PER_VCPU_HOUR = 0.12   # $0.12 / vCPU-hour  (approx AWS m5.large spot)


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class Suggestion:
    rule_name: str
    severity: str          # CRITICAL | HIGH | MEDIUM | LOW
    message: str
    detail: str


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(job: JobLog) -> List[Suggestion]:
    """Return a list of actionable suggestions for this job."""
    suggestions: List[Suggestion] = []
    stages = job.stages

    total_shuffle = sum(s.shuffle_read_bytes + s.shuffle_write_bytes for s in stages)
    total_spill   = sum(s.total_spill_bytes for s in stages)

    # ── Rule 1: High shuffle ─────────────────────────────────────────────────
    if total_shuffle > 1_073_741_824:   # > 1 GB
        suggestions.append(Suggestion(
            rule_name="HIGH_SHUFFLE",
            severity="HIGH",
            message=f"Large shuffle volume: {_fmt(total_shuffle)}",
            detail=(
                "Consider enabling broadcast join for small tables "
                "(spark.sql.autoBroadcastJoinThreshold, default 10 MB). "
                "Pre-partition both sides on the join key before the join. "
                "Enable AQE: spark.sql.adaptive.enabled=true."
            ),
        ))

    # ── Rule 2: Memory / disk spill ──────────────────────────────────────────
    if total_spill > 0:
        sev = "CRITICAL" if total_spill > 1_073_741_824 else "HIGH"
        suggestions.append(Suggestion(
            rule_name="MEMORY_SPILL",
            severity=sev,
            message=f"Spill to disk detected: {_fmt(total_spill)}",
            detail=(
                "Increase executor memory (spark.executor.memory). "
                "Increase spark.sql.shuffle.partitions to reduce per-partition size. "
                "Use spark.memory.fraction and spark.memory.storageFraction to tune "
                "the execution/storage split."
            ),
        ))

    # ── Rule 3: Data skew (per stage) ────────────────────────────────────────
    for stage in stages:
        if len(stage.task_durations_ms) > 1 and stage.skew_ratio > 5:
            suggestions.append(Suggestion(
                rule_name="DATA_SKEW",
                severity="HIGH",
                message=(
                    f"Skew in stage '{stage.stage_name}': "
                    f"max task {stage.max_task_ms} ms vs median {stage.median_task_ms:.0f} ms "
                    f"({stage.skew_ratio:.1f}x)"
                ),
                detail=(
                    "Enable AQE skew-join: spark.sql.adaptive.skewJoin.enabled=true. "
                    "Salt heavily-skewed keys before joining. "
                    "Repartition on a higher-cardinality column."
                ),
            ))
            break   # one skew suggestion per job is enough

    # ── Rule 4: Small files / too many tiny partitions ───────────────────────
    for stage in stages:
        if stage.num_tasks > 50 and stage.input_bytes > 0:
            avg_bytes = stage.input_bytes / stage.num_tasks
            if avg_bytes < 1_048_576:   # < 1 MB per task
                suggestions.append(Suggestion(
                    rule_name="SMALL_FILES",
                    severity="MEDIUM",
                    message=(
                        f"Stage '{stage.stage_name}' has {stage.num_tasks} tasks "
                        f"with only {_fmt(int(avg_bytes))} avg input each"
                    ),
                    detail=(
                        "Compact small files at the source with OPTIMIZE / compaction. "
                        "Use coalesce() or repartition() to reduce partition count. "
                        "Increase spark.sql.files.maxPartitionBytes (default 128 MB)."
                    ),
                ))
                break

    # ── Rule 5: Low CPU utilization ───────────────────────────────────────────
    for stage in stages:
        if (
            stage.duration_ms > 10_000
            and stage.executor_run_time_ms > 0
            and stage.num_tasks > 0
        ):
            # Ideal CPU: if all tasks ran in parallel for the full stage duration
            ideal_cpu_ms = stage.duration_ms * stage.num_tasks
            utilization = stage.executor_run_time_ms / ideal_cpu_ms
            if utilization < 0.30:
                suggestions.append(Suggestion(
                    rule_name="LOW_PARALLELISM",
                    severity="MEDIUM",
                    message=(
                        f"Low CPU utilisation in stage '{stage.stage_name}': "
                        f"{utilization:.0%}"
                    ),
                    detail=(
                        "Increase spark.default.parallelism and "
                        "spark.sql.shuffle.partitions. "
                        "Check for GC overhead (add -verbose:gc to extraJavaOptions). "
                        "Profile with the Spark Web UI flame graph."
                    ),
                ))
                break

    return suggestions


def waste_score(job: JobLog) -> float:
    """Compute a 0–100 waste score. Higher = more wasteful."""
    stages = job.stages
    total_shuffle = sum(s.shuffle_read_bytes + s.shuffle_write_bytes for s in stages)
    total_spill   = sum(s.total_spill_bytes for s in stages)
    max_skew      = max((s.skew_ratio for s in stages), default=1.0)

    shuffle_pts = min(total_shuffle / 1_073_741_824 * 5, 30)   # 0–30
    spill_pts   = min(total_spill   / 1_073_741_824 * 15, 30)  # 0–30
    dur_pts     = min(job.duration_ms / 60_000 * 2, 20)        # 0–20
    skew_pts    = min(max(0.0, max_skew - 1) * 5, 20)          # 0–20

    return round(min(shuffle_pts + spill_pts + dur_pts + skew_pts, 100.0), 2)


def cost_usd(job: JobLog, num_executors: int = 1) -> float:
    """Estimate job cost in USD based on total executor CPU time."""
    total_exec_ms = sum(s.executor_run_time_ms for s in job.stages)
    cpu_hours = (total_exec_ms / 3_600_000) * num_executors
    return round(cpu_hours * _RATE_PER_VCPU_HOUR, 6)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b //= 1024
    return f"{b:.1f} PB"
