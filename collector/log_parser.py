"""
Spark Event Log Parser
======================
Handles two Spark event log formats:

  1. Flat file  (classic)  — one JSON object per line, plain or gzipped
     Written when spark.eventLog.dir points to a cluster/HDFS location.
     e.g.  app-20240601-120000-0001

  2. eventlog_v2 directory (Spark 3.4+ local mode)
     A directory named  eventlog_v2_<appId>/  containing:
       appstatus_<appId>                   — presence means app finished
       events_1_<appId>.zstd              — zstd-compressed JSON events
     (snappy / lz4 variants also handled)
"""
from __future__ import annotations

import io
import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Optional


# ─── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class StageInfo:
    stage_id: int
    stage_name: str = ""
    num_tasks: int = 0
    duration_ms: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    memory_spill_bytes: int = 0
    disk_spill_bytes: int = 0
    executor_run_time_ms: int = 0
    task_durations_ms: list = field(default_factory=list)

    @property
    def total_spill_bytes(self) -> int:
        return self.memory_spill_bytes + self.disk_spill_bytes

    @property
    def max_task_ms(self) -> int:
        return max(self.task_durations_ms) if self.task_durations_ms else 0

    @property
    def median_task_ms(self) -> float:
        return statistics.median(self.task_durations_ms) if self.task_durations_ms else 0.0

    @property
    def skew_ratio(self) -> float:
        med = self.median_task_ms
        return (self.max_task_ms / med) if med > 0 else 1.0


@dataclass
class JobLog:
    app_id: str
    app_name: str
    start_time: int
    end_time: int = 0
    duration_ms: int = 0
    status: str = "UNKNOWN"
    stages: list = field(default_factory=list)
    log_file: str = ""   # path used as a unique processing key in the DB


# ─── Public API ───────────────────────────────────────────────────────────────

def parse_event_log(path: str) -> Optional[JobLog]:
    """
    Parse a Spark event log at *path* (file or eventlog_v2 directory).
    Returns None if the log is incomplete, unreadable, or in an unknown format.
    """
    p = Path(path)
    if p.is_dir():
        return _parse_v2_dir(p)
    return _parse_flat_file(p)


def is_complete_log(path: str) -> bool:
    """Return True if the log at *path* looks like a finished application."""
    p = Path(path)
    if p.is_dir():
        return _v2_dir_is_complete(p)
    # Flat file: no .inprogress suffix = complete
    return p.suffix != ".inprogress"


# ─── Flat-file parser ─────────────────────────────────────────────────────────

def _parse_flat_file(path: Path) -> Optional[JobLog]:
    if path.suffix == ".inprogress":
        return None
    try:
        lines: Iterable[str] = path.open("r", encoding="utf-8", errors="replace")
        job, stages = _process_events(lines, log_file=str(path))
    except OSError as exc:
        print(f"[parser] Cannot read {path}: {exc}")
        return None
    if job is None:
        return None
    job.stages = list(stages.values())
    return job


# ─── eventlog_v2 directory parser ─────────────────────────────────────────────

def _v2_dir_is_complete(d: Path) -> bool:
    """
    A v2 log directory is complete when:
      - At least one appstatus_* file exists (created when the app ends)
      - At least one events_* file exists WITHOUT an .inprogress component
    """
    has_status = any(f.name.startswith("appstatus_") for f in d.iterdir())
    has_events = any(
        f.name.startswith("events_") and ".inprogress" not in f.name
        for f in d.iterdir()
    )
    return has_status and has_events


def _parse_v2_dir(d: Path) -> Optional[JobLog]:
    if not _v2_dir_is_complete(d):
        return None

    # Collect all completed events files, sorted for deterministic order
    event_files = sorted(
        f for f in d.iterdir()
        if f.name.startswith("events_") and ".inprogress" not in f.name
    )

    job: Optional[JobLog] = None
    stages: dict[int, StageInfo] = {}

    for ef in event_files:
        try:
            for line in _decompress_lines(ef):
                j, stages = _process_event_line(line, str(d), job, stages)
                if j is not None:
                    job = j
        except Exception as exc:
            print(f"[parser] Error reading {ef}: {exc}")
            continue

    if job is None:
        return None
    job.stages = list(stages.values())
    return job


def _decompress_lines(path: Path) -> Iterator[str]:
    """Yield decoded text lines from a plain, .zstd, .snappy, or .lz4 events file."""
    suffix = path.suffix.lower()

    if suffix == ".zstd":
        import zstandard
        dctx = zstandard.ZstdDecompressor()
        with path.open("rb") as fh:
            with dctx.stream_reader(fh) as reader:
                yield from io.TextIOWrapper(reader, encoding="utf-8", errors="replace")

    elif suffix == ".snappy":
        try:
            import snappy
            raw = path.read_bytes()
            data = snappy.decompress(raw)
            yield from data.decode("utf-8", errors="replace").splitlines()
        except ImportError:
            print(f"[parser] Install python-snappy to read {path.name}")

    elif suffix == ".lz4":
        try:
            import lz4.frame
            raw = path.read_bytes()
            data = lz4.frame.decompress(raw)
            yield from data.decode("utf-8", errors="replace").splitlines()
        except ImportError:
            print(f"[parser] Install lz4 to read {path.name}")

    else:
        # Uncompressed or unknown — try as plain text
        yield from path.open("r", encoding="utf-8", errors="replace")


# ─── Shared event-processing logic ───────────────────────────────────────────

def _process_events(
    lines: Iterable[str],
    log_file: str,
) -> tuple[Optional[JobLog], dict[int, StageInfo]]:
    """Iterate over text lines and build JobLog + stage dict."""
    job: Optional[JobLog] = None
    stages: dict[int, StageInfo] = {}

    for raw in lines:
        j, stages = _process_event_line(raw.strip(), log_file, job, stages)
        if j is not None:
            job = j

    return job, stages


def _process_event_line(
    line: str,
    log_file: str,
    job: Optional[JobLog],
    stages: dict[int, StageInfo],
) -> tuple[Optional[JobLog], dict[int, StageInfo]]:
    """Process a single JSON event line. Returns (updated_job, updated_stages)."""
    if not line:
        return job, stages
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        return job, stages

    etype = ev.get("Event", "")

    # ── Application lifecycle ──────────────────────────────────────────────────
    if etype == "SparkListenerApplicationStart":
        job = JobLog(
            app_id=ev.get("App ID", ""),
            app_name=ev.get("App Name", "unknown"),
            start_time=ev.get("Timestamp", 0),
            log_file=log_file,
        )

    elif etype == "SparkListenerApplicationEnd" and job:
        job.end_time = ev.get("Timestamp", 0)
        job.duration_ms = max(0, job.end_time - job.start_time)
        job.status = "COMPLETED"

    # ── Stage completed ────────────────────────────────────────────────────────
    elif etype == "SparkListenerStageCompleted":
        si  = ev.get("Stage Info", {})
        sid = si.get("Stage ID", 0)
        stage = stages.get(sid) or StageInfo(stage_id=sid)

        stage.stage_name = si.get("Stage Name", "")
        stage.num_tasks  = si.get("Number of Tasks", 0)

        sub = si.get("Submission Time", 0)
        cmp = si.get("Completion Time", 0)
        if sub and cmp:
            stage.duration_ms = max(0, cmp - sub)

        stages[sid] = stage

    # ── Task end — accumulate per-stage metrics ────────────────────────────────
    elif etype == "SparkListenerTaskEnd":
        sid = ev.get("Stage ID", 0)
        if sid not in stages:
            stages[sid] = StageInfo(stage_id=sid)
        stage = stages[sid]

        tm = ev.get("Task Metrics", {})
        if not tm:
            return job, stages

        # Shuffle read — Spark splits into Remote + Local (no "Total Bytes Read" key)
        srm = tm.get("Shuffle Read Metrics", {})
        stage.shuffle_read_bytes += (
            srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)
        )

        swm = tm.get("Shuffle Write Metrics", {})
        stage.shuffle_write_bytes += swm.get("Shuffle Bytes Written", 0)

        # Spill
        stage.memory_spill_bytes += tm.get("Memory Bytes Spilled", 0)
        stage.disk_spill_bytes   += tm.get("Disk Bytes Spilled", 0)

        # Executor time
        stage.executor_run_time_ms += tm.get("Executor Run Time", 0)

        # I/O
        stage.input_bytes  += tm.get("Input Metrics",  {}).get("Bytes Read",    0)
        stage.output_bytes += tm.get("Output Metrics", {}).get("Bytes Written", 0)

        # Task wall-clock duration (for skew detection)
        ti     = ev.get("Task Info", {})
        finish = ti.get("Finish Time", 0)
        launch = ti.get("Launch Time", 0)
        if finish and launch and finish > launch:
            stage.task_durations_ms.append(finish - launch)

    return job, stages
