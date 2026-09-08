#!/usr/bin/env python3
"""Validate ClusterSentinel observability artifacts (alerts YAML + dashboards JSON)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - fallback without PyYAML
    yaml = None

ROOT = Path(__file__).resolve().parents[1]
ALERTS = ROOT / "observability" / "alerts" / "clustersentinel.yaml"
DASHBOARDS = [ROOT / "observability" / "dashboards" / "clustersentinel.json"]

REQUIRED_ALERTS = {
    "ClusterSentinelStorageWriteErrors",
    "ClusterSentinelStorageWritesStalled",
    "ClusterSentinelStorageCheckpointStale",
    "ClusterSentinelStoragePvcHighUsage",
}

REQUIRED_METRIC_SNIPPETS = [
    "clustersentinel_storage_writes_total",
    "clustersentinel_storage_write_duration_seconds",
    "clustersentinel_storage_errors_total",
    "clustersentinel_storage_rows",
    "clustersentinel_storage_bytes",
    "clustersentinel_storage_pruned_total",
    "clustersentinel_watch_checkpoint_age_seconds",
]


def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def load_alerts() -> dict:
    text = ALERTS.read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            fail("alerts YAML root must be a mapping")
        return data
    # Minimal structural checks without PyYAML.
    if "groups:" not in text:
        fail("alerts YAML missing groups")
    missing = {name for name in REQUIRED_ALERTS if f"alert: {name}" not in text}
    if missing:
        fail(f"alerts YAML missing required rules: {sorted(missing)}")
    return {"_raw": True}


def main() -> None:
    if not ALERTS.is_file():
        fail(f"missing {ALERTS}")
    data = load_alerts()
    if "_raw" not in data:
        names: set[str] = set()
        for group in data.get("groups") or []:
            for rule in group.get("rules") or []:
                if "alert" in rule:
                    names.add(rule["alert"])
        missing = REQUIRED_ALERTS - names
        if missing:
            fail(f"missing alert rules: {sorted(missing)}")
        print(f"OK alerts: {ALERTS} ({len(names)} rules)")
    else:
        print(f"OK alerts (raw check): {ALERTS}")

    for path in DASHBOARDS:
        if not path.is_file():
            fail(f"missing {path}")
        dash = json.loads(path.read_text(encoding="utf-8"))
        if "uid" not in dash or "title" not in dash:
            fail(f"{path} missing uid/title")
        blob = json.dumps(dash)
        for snippet in REQUIRED_METRIC_SNIPPETS:
            if snippet not in blob:
                fail(f"{path.name} missing panel metric {snippet}")
        if "Storage" not in blob:
            fail(f"{path.name} missing Storage row")
        print(f"OK dashboard: {path.name} uid={dash['uid']} title={dash['title']}")

    print("observability validation passed")


if __name__ == "__main__":
    main()
