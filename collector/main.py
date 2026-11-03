"""
SparkLens Collector
===================
Polls the Spark event log directory, parses completed log files,
and persists metrics + suggestions to PostgreSQL.
"""
from __future__ import annotations

import glob
import os
import time

from db import make_engine, get_processed_files, save_job
from log_parser import parse_event_log, is_complete_log

EVENT_LOG_DIR = os.getenv("EVENT_LOG_DIR", "/spark-events")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))


def scan_once(engine) -> int:
    processed = get_processed_files(engine)
    new_count = 0

    all_entries = glob.glob(os.path.join(EVENT_LOG_DIR, "*"))
    candidates = [
        f for f in all_entries
        if f not in processed and is_complete_log(f)
    ]

    for log_file in candidates:
        try:
            job = parse_event_log(log_file)
            if job is None:
                continue
            if job.status != "COMPLETED":
                print(f"[collector] Skip (not completed): {os.path.basename(log_file)}")
                continue
            save_job(job, engine)
            new_count += 1
            print(
                f"[collector] Saved  '{job.app_name}' "
                f"({job.app_id})  duration={job.duration_ms}ms"
            )
        except Exception as exc:
            print(f"[collector] ERROR processing {log_file}: {exc}")

    return new_count


def main() -> None:
    print("=" * 56)
    print("  SparkLens Collector")
    print(f"  Watching : {EVENT_LOG_DIR}")
    print(f"  Interval : {POLL_INTERVAL}s")
    print("=" * 56)

    engine = make_engine()

    while True:
        try:
            n = scan_once(engine)
            if n == 0:
                print(f"[collector] No new logs. Sleeping {POLL_INTERVAL}s …")
        except Exception as exc:
            print(f"[collector] Loop error: {exc}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
