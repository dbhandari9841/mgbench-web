#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def normalize_source(row: Dict[str, Any]) -> str:
    src = str(row.get("source") or "").strip().lower()
    if src == "standalone_mg_benchmark":
        return "standalone"
    if src == "manual_benchmark_patched":
        return "athena_manual"
    if src in {"standalone", "athena_manual"}:
        return src
    return "unknown"


def normalize_backend(row: Dict[str, Any]) -> str:
    raw_csv = row.get("raw_csv") or {}
    candidates = [
        row.get("backend"), raw_csv.get("backend_label"), raw_csv.get("backend_requested"),
        raw_csv.get("backend_used"), raw_csv.get("jo_dir"), raw_csv.get("outtag"),
        row.get("csv_path"), row.get("log_generate_path"),
    ]
    text = " ".join(str(x) for x in candidates if x).lower()
    if "cpp512y" in text: return "cpp512y"
    if "cpp512z" in text: return "cpp512z"
    if "cppavx2" in text or " avx2" in text: return "cppavx2"
    if "cppnone" in text: return "cppnone"
    if "madevent_gpu" in text or "_gpu" in text or ".gpu." in text or " cuda" in text or text == "gpu": return "cuda"
    if "fortran" in text: return "fortran"
    value = row.get("backend")
    return str(value) if value not in (None, "") else "unknown"


def normalize_jets(row: Dict[str, Any]) -> Optional[int]:
    raw_csv = row.get("raw_csv") or {}
    candidates = [row.get("jets"), raw_csv.get("jets"), raw_csv.get("njet"), raw_csv.get("njets")]
    for candidate in candidates:
        if candidate in (None, ""): continue
        s = str(candidate).strip()
        if s.isdigit(): return int(s)
        m = re.search(r"(\d+)(?:\s*to\s*(\d+))?\s*j", s, re.IGNORECASE)
        if m: return int(m.group(2) or m.group(1))
        m = re.search(r"^(\d+)\s*to\s*(\d+)$", s, re.IGNORECASE)
        if m: return int(m.group(2))
    haystack = " ".join(str(x) for x in [raw_csv.get("jo_dir"), raw_csv.get("outtag"), raw_csv.get("process"), raw_csv.get("proc"), raw_csv.get("process_name"), row.get("csv_path"), row.get("log_generate_path"), row.get("run_id")] if x)
    for pat in [r"(?:^|[^\d])(\d+)\s*to\s*(\d+)\s*j(?:[^a-z0-9]|$)", r"(?:^|[^\d])(\d+)to(\d+)j(?:[^a-z0-9]|$)", r"(?:^|[^\d])(\d+)j(?:[^a-z0-9]|$)", r"(?:^|[^\d])(\d+)(?:to(\d+))?J\b"]:
        m = re.search(pat, haystack, re.IGNORECASE)
        if m: return int(m.group(2) or m.group(1))
    return None


def to_int(value: Any) -> Optional[int]:
    if value in (None, ""): return None
    try: return int(float(str(value).strip()))
    except Exception: return None


def to_float(value: Any) -> Optional[float]:
    if value in (None, ""): return None
    try:
        out = float(str(value).strip())
        if math.isnan(out) or math.isinf(out): return None
        return out
    except Exception:
        return None


def clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    raw_csv = row.get("raw_csv") or {}
    cleaned = dict(row)
    cleaned["source_norm"] = normalize_source(row)
    cleaned["backend_norm"] = normalize_backend(row)
    cleaned["jets_norm"] = normalize_jets(row)
    cleaned["events_norm"] = to_int(row.get("events") or raw_csv.get("nevt_req") or raw_csv.get("nevt_done") or raw_csv.get("nevt"))
    cleaned["wall_s_norm"] = to_float(row.get("wall_s"))
    cleaned["status_norm"] = str(row.get("status") or "unknown")
    cleaned["process_norm"] = str(row.get("process") or "unknown")
    cleaned["mode_norm"] = str(row.get("mode") or "unknown")
    cleaned["mg_norm"] = str(row.get("madgraph_version") or "unknown")
    cleaned["athena_norm"] = str(row.get("athena_version") or "unknown")
    cleaned["patch_norm"] = "shadow" if row.get("patch_shadow") else "none"
    cleaned["hardware_norm"] = str(row.get("gpu_name") or row.get("cpu_name") or row.get("node") or "unknown")
    return cleaned


def summarize_group(rows: List[Dict[str, Any]], x_field: str) -> Optional[Dict[str, Any]]:
    x = rows[0].get(x_field)
    ys = [r["wall_s_norm"] for r in rows if r.get("wall_s_norm") is not None]
    if x is None or not ys: return None
    med = statistics.median(ys)
    return {
        "x": x, "y": med, "n_runs": len(ys), "y_min": min(ys), "y_max": max(ys),
        "y_mean": statistics.mean(ys), "y_err_low": med - min(ys), "y_err_high": max(ys) - med,
    }


def make_sweeps(rows: List[Dict[str, Any]], x_field: str, fixed_fields: List[str]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["status_norm"] != "ok" or row.get("wall_s_norm") is None or row.get(x_field) is None:
            continue
        grouped[tuple(row.get(f) for f in fixed_fields)].append(row)

    sweeps: List[Dict[str, Any]] = []
    for key, members in grouped.items():
        x_groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for row in members: x_groups[row[x_field]].append(row)
        points = []
        for xval in sorted(x_groups.keys()):
            summary = summarize_group(x_groups[xval], x_field)
            if summary is not None: points.append(summary)
        if len(points) < 2: continue
        meta = {field: value for field, value in zip(fixed_fields, key)}
        if x_field == "events_norm":
            fixed_bit = f"{meta.get('jets_norm')}J" if meta.get('jets_norm') is not None else None
        else:
            fixed_bit = f"{meta.get('events_norm')} evt" if meta.get('events_norm') is not None else None
        label_bits = [meta.get("source_norm"), meta.get("process_norm"), fixed_bit, meta.get("mode_norm"), meta.get("backend_norm"), meta.get("mg_norm")]
        label = " | ".join(str(x) for x in label_bits if x not in (None, "unknown", "None"))
        sweeps.append({"label": label, "x_field": x_field, "points": points, "meta": meta})
    sweeps.sort(key=lambda s: (str(s["meta"].get("source_norm")), str(s["meta"].get("process_norm")), str(s["meta"].get("backend_norm")), str(s["label"])))
    return sweeps


def build_output(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    cleaned = [clean_row(r) for r in rows]
    ok_rows = [r for r in cleaned if r["status_norm"] == "ok"]
    def values(field: str):
        return sorted({r[field] for r in cleaned if r.get(field) not in (None, "", "unknown")}, key=str)
    sweeps_events = make_sweeps(cleaned, "events_norm", ["source_norm", "process_norm", "jets_norm", "mode_norm", "backend_norm", "mg_norm", "athena_norm", "patch_norm"])
    sweeps_jets = make_sweeps(cleaned, "jets_norm", ["source_norm", "process_norm", "events_norm", "mode_norm", "backend_norm", "mg_norm", "athena_norm", "patch_norm"])
    return {
        "meta": {"n_rows_total": len(cleaned), "n_rows_ok": len(ok_rows), "n_sweeps_events": len(sweeps_events), "n_sweeps_jets": len(sweeps_jets)},
        "filters": {"source": values("source_norm"), "process": values("process_norm"), "backend": values("backend_norm"), "mode": values("mode_norm"), "mg": values("mg_norm"), "athena": values("athena_norm"), "patch": values("patch_norm")},
        "plots": {
            "time_vs_events": {"title": "Wall time vs number of events", "x_title": "Events", "y_title": "Wall time (s)", "sweeps": sweeps_events},
            "time_vs_jets": {"title": "Wall time vs number of jets", "x_title": "Jets", "y_title": "Wall time (s)", "sweeps": sweeps_jets},
        },
    }


def main() -> int:
    if len(sys.argv) < 2:
        print(f"Usage: {Path(sys.argv[0]).name} COMBINED_CACHE.json", file=sys.stderr)
        return 2
    cache_path = Path(sys.argv[1])
    if not cache_path.is_file():
        print(json.dumps({"error": f"Not a file: {cache_path}"}), file=sys.stderr)
        return 2
    rows = json.loads(cache_path.read_text(encoding="utf-8", errors="replace"))
    if not isinstance(rows, list):
        print(json.dumps({"error": "cache is not a JSON array"}), file=sys.stderr)
        return 2
    print(json.dumps(build_output(rows), separators=(",", ":")))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())