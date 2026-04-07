#!/usr/bin/env python3
"""
Scan MG_standalone_lhe_shower-style logs and emit normalized benchmark JSON.

This scanner is meant for standalone four-backend runs where a single Condor job
executes backends in this fixed order:
    fortran, cppnone, cppavx2, cuda

Benchmark times are taken from the 2nd, 4th, 6th, and 8th `real` entries in
log*.err.

Typical inputs per job:
  - log-mg.<cluster>.log      (Condor event log)
  - log-mg.<cluster>.<proc>.out
  - log-mg.<cluster>.<proc>.err
  - optionally nearby input_card_a*.txt / input_card_b*.txt

Output rows are normalized to look similar to the manual benchmark cache rows so
that they can be merged into the same web app.
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

signal.signal(signal.SIGPIPE, signal.SIG_DFL)

BACKENDS = ["fortran", "cppnone", "cppavx2", "cuda"]

OUT_RE = re.compile(r"log-mg\.(\d+)\.(\d+)\.out$")
ERR_RE = re.compile(r"log-mg\.(\d+)\.(\d+)\.err$")
CONDOR_LOG_RE = re.compile(r"log-mg\.(\d+)\.log$")

PROCESS_RE = re.compile(r"\b(DY|TT)\b", re.IGNORECASE)

JET_PATTERNS = [
    re.compile(r"(?:^|[^0-9])(\d+)\s*to\s*(\d+)\s*j(?:[^a-z0-9]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[^0-9])(\d+)to(\d+)j(?:[^a-z0-9]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[^0-9])(\d+)j(?:[^a-z0-9]|$)", re.IGNORECASE),
    re.compile(r"_(\d+)(?:to(\d+))?J\b", re.IGNORECASE),
]

REAL_RE = re.compile(r"^real\s+(\d+)m([0-9.]+)s\s*$", re.MULTILINE)

HOST_RE = re.compile(r"Running on host:\s*(\S+)")
START_RE = re.compile(r"Start time:\s*(.+)")
END_RE = re.compile(r"End time:\s*(.+)")
CUDA_ARCH_RE = re.compile(r"Using CUDA architecture:\s*([^\n]+)")
PYTHON_RE = re.compile(r"Using Python:\s*([^\n]+)")
MG_BASE_RE = re.compile(r"Using MG base:\s*([^\n]+)")
PLUGIN_RE = re.compile(r"Using CUDACPP plugin:\s*([^\n]+)")
EVENTS_RE = re.compile(r"(?:nevents|set\s+nevents)\s*[=:]?\s*([0-9]+)", re.IGNORECASE)

BACKEND_START_RE = re.compile(r">>> Starting backend:\s*(\w+)")
BACKEND_DONE_RE = re.compile(r"[✔✓]\s*Backend\s+(\w+)\s+completed successfully\.?", re.IGNORECASE)

CONDOR_EXEC_RE = re.compile(
    r"001 \((\d+)\.(\d+)\.\d+\)\s+([0-9/]+\s+[0-9:]+)\s+Job executing on host:.*?alias=(\S+?)&",
    re.DOTALL,
)
SLOT_RE = re.compile(r"\bSlotName:\s*(\S+)")
CPU_COUNT_RE = re.compile(r"\bCpus\s*=\s*(\d+)")
MEM_REQ_RE = re.compile(r"\bMemory\s*=\s*(\d+)")
GPU_DEVICE_RE = re.compile(r'DeviceName = "([^"]+)"')
TIMEEXEC_RE = re.compile(r"TimeExecute \(s\)\s*:\s*(\d+)")

IMAGE_MEM_RE = re.compile(r"\n\t(\d+)\s*-\s*MemoryUsage of job \(MB\)")
RSS_RE = re.compile(r"\n\t(\d+)\s*-\s*ResidentSetSize of job \(KB\)")

HELD_RE = re.compile(r"\b012 \((\d+)\.(\d+)\.\d+\).+?Job was held\.", re.DOTALL)
ABORT_RE = re.compile(r"\b009 \((\d+)\.(\d+)\.\d+\).+?Job was aborted\.", re.DOTALL)
EVICT_RE = re.compile(r"\b004 \((\d+)\.(\d+)\.\d+\).+?Job was evicted\.", re.DOTALL)
ERROR_TRANSFER_RE = re.compile(r"max total download bytes exceeded", re.IGNORECASE)

CPU_MODEL_PATTERNS = [
    re.compile(r"CPUModelName\s*=\s*(.+)"),
    re.compile(r"model name\s*:\s*(.+)", re.IGNORECASE),
    re.compile(r"Intel\(R\).+?CPU.+", re.IGNORECASE),
    re.compile(r"AMD EPYC.+", re.IGNORECASE),
]

REQUEST_GPUS_RE = re.compile(r"request_gpus\s*=\s*(\d+)", re.IGNORECASE)
REQUEST_CPUS_RE = re.compile(r"request_cpus\s*=\s*(\d+)", re.IGNORECASE)
REQUEST_MEMORY_RE = re.compile(r"request_memory\s*=\s*([0-9]+\s*[A-Za-z]+)", re.IGNORECASE)


def read_text_safe(path: Path, max_bytes: int = 20_000_000) -> str:
    try:
        data = path.read_bytes()
        if len(data) > max_bytes:
            data = data[:max_bytes]
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def parse_real_times(err_text: str) -> List[Optional[float]]:
    times: List[float] = []
    for m in REAL_RE.finditer(err_text):
        minutes = int(m.group(1))
        seconds = float(m.group(2))
        times.append(minutes * 60.0 + seconds)

    wanted_indices = [1, 3, 5, 7]
    out: List[Optional[float]] = []
    for idx in wanted_indices:
        out.append(times[idx] if idx < len(times) else None)
    return out


def parse_jets_and_mode_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None

    lowered = text.lower()

    for pat in JET_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        low = m.group(1)
        high = m.group(2) if m.lastindex and m.lastindex >= 2 else None
        if high is not None:
            return str(high), "inclusive"
        return str(low), "exclusive"

    if "inclusive" in lowered:
        return None, "inclusive"
    if "exclusive" in lowered:
        return None, "exclusive"

    return None, None


def infer_process_jets_mode_from_texts(*texts: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    process = None
    jets = None
    mode = None

    for text in texts:
        if not text:
            continue

        if process is None:
            m = PROCESS_RE.search(text)
            if m:
                process = m.group(1).upper()

        if jets is None or mode is None:
            j, mo = parse_jets_and_mode_from_text(text)
            if jets is None and j is not None:
                jets = j
            if mode is None and mo is not None:
                mode = mo

    return process, jets, mode


def parse_dt_any(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    for fmt in [
        "%a %b %d %I:%M:%S %p %Z %Y",
        "%a %b %d %H:%M:%S %Z %Y",
    ]:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            pass
    return s


def condor_timestamp_to_iso(ts: str, year: int = 2026) -> Optional[str]:
    try:
        dt = datetime.strptime(f"{year}/{ts}", "%Y/%m/%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def infer_mg_version_label(mg_base: Optional[str]) -> Optional[str]:
    if not mg_base:
        return None

    base = Path(mg_base.strip()).name.lower()
    path = mg_base.lower()

    if "mg5amcnlo_dev" in path:
        return "3.6.7_dev"
    if "mg5amcnlo_stable" in path:
        return "3.6.7_stable"

    m = re.search(r"v?([0-9]+\.[0-9]+\.[0-9]+(?:_[a-z]+)?)", path)
    if m:
        return m.group(1)

    return base or None


def extract_first_match(text: str, patterns: List[re.Pattern[str]]) -> Optional[str]:
    for pat in patterns:
        m = pat.search(text)
        if m:
            if m.lastindex:
                return m.group(1).strip()
            return m.group(0).strip()
    return None


def parse_events_from_texts(*texts: str) -> Optional[int]:
    for text in texts:
        if not text:
            continue
        m = EVENTS_RE.search(text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


def find_nearby_cards(out_path: Path) -> List[Path]:
    candidates: List[Path] = []

    search_dirs = [out_path.parent]
    if out_path.parent.parent != out_path.parent:
        search_dirs.append(out_path.parent.parent)

    names = [
        "input_card_a.txt",
        "input_card_b.txt",
        "input_card_a_fortran.txt",
        "input_card_b_fortran.txt",
        "input_card_a_cppnone.txt",
        "input_card_b_cppnone.txt",
        "input_card_a_cppavx2.txt",
        "input_card_b_cppavx2.txt",
        "input_card_a_cuda.txt",
        "input_card_b_cuda.txt",
    ]

    for d in search_dirs:
        for name in names:
            p = d / name
            if p.is_file():
                candidates.append(p)

        for p in d.glob("input_card_*.txt"):
            if p.is_file():
                candidates.append(p)

    deduped = []
    seen = set()
    for p in candidates:
        s = str(p.resolve()) if p.exists() else str(p)
        if s not in seen:
            seen.add(s)
            deduped.append(p)

    return deduped


def summarize_condor_log(text: str) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "node": None,
        "slot": None,
        "cpu_count": None,
        "mem_request_mb": None,
        "gpu_name": None,
        "gpu_count": None,
        "cpu_name": None,
        "condor_status": "unknown",
        "condor_hold_reason": None,
        "time_execute_s": None,
        "max_memory_mb": None,
        "max_rss_kb": None,
        "execute_started": None,
    }

    exec_m = CONDOR_EXEC_RE.search(text)
    if exec_m:
        summary["execute_started"] = condor_timestamp_to_iso(exec_m.group(3))
        summary["node"] = exec_m.group(4)

    slot_m = SLOT_RE.search(text)
    if slot_m:
        summary["slot"] = slot_m.group(1)

    cpu_m = CPU_COUNT_RE.search(text)
    if cpu_m:
        summary["cpu_count"] = int(cpu_m.group(1))

    mem_m = MEM_REQ_RE.search(text)
    if mem_m:
        summary["mem_request_mb"] = int(mem_m.group(1))

    gpu_names = GPU_DEVICE_RE.findall(text)
    if gpu_names:
        summary["gpu_name"] = gpu_names[0]
        summary["gpu_count"] = len(gpu_names)

    summary["cpu_name"] = extract_first_match(text, CPU_MODEL_PATTERNS)

    texec = [int(x) for x in TIMEEXEC_RE.findall(text)]
    if texec:
        summary["time_execute_s"] = max(texec)

    mems = [int(x) for x in IMAGE_MEM_RE.findall(text)]
    if mems:
        summary["max_memory_mb"] = max(mems)

    rss = [int(x) for x in RSS_RE.findall(text)]
    if rss:
        summary["max_rss_kb"] = max(rss)

    if HELD_RE.search(text) or ABORT_RE.search(text) or EVICT_RE.search(text):
        summary["condor_status"] = "failed"
    elif "Job terminated" in text or "job terminated by itself" in text.lower():
        summary["condor_status"] = "ok"

    if ERROR_TRANSFER_RE.search(text):
        summary["condor_hold_reason"] = "max_download_bytes_exceeded"

    return summary


def infer_requested_resources(*texts: str) -> Dict[str, Any]:
    out = {
        "request_gpus": None,
        "request_cpus": None,
        "request_memory": None,
    }

    for text in texts:
        if not text:
            continue

        if out["request_gpus"] is None:
            m = REQUEST_GPUS_RE.search(text)
            if m:
                out["request_gpus"] = int(m.group(1))

        if out["request_cpus"] is None:
            m = REQUEST_CPUS_RE.search(text)
            if m:
                out["request_cpus"] = int(m.group(1))

        if out["request_memory"] is None:
            m = REQUEST_MEMORY_RE.search(text)
            if m:
                out["request_memory"] = m.group(1).strip()

    return out


def build_rows_for_job(out_path: Path, err_path: Path, condor_log_path: Optional[Path]) -> List[Dict[str, Any]]:
    out_text = read_text_safe(out_path)
    err_text = read_text_safe(err_path)
    condor_text = read_text_safe(condor_log_path) if condor_log_path else ""

    out_m = OUT_RE.search(out_path.name)
    if not out_m:
        return []

    cluster_id, proc_id = out_m.group(1), out_m.group(2)
    run_id = f"{cluster_id}.{proc_id}"

    card_paths = find_nearby_cards(out_path)
    card_texts = [read_text_safe(p, max_bytes=2_000_000) for p in card_paths]

    benchmark_times = parse_real_times(err_text)
    condor = summarize_condor_log(condor_text)
    req_resources = infer_requested_resources(out_text, err_text, condor_text, *card_texts)

    host = HOST_RE.search(out_text)
    start = START_RE.search(out_text)
    end = END_RE.search(out_text)
    cuda_arch = CUDA_ARCH_RE.search(out_text)
    python_v = PYTHON_RE.search(out_text)
    mg_base = MG_BASE_RE.search(out_text)
    plugin = PLUGIN_RE.search(out_text)

    process, jets, mode = infer_process_jets_mode_from_texts(
        str(out_path),
        str(err_path),
        str(condor_log_path or ""),
        str(out_path.parent),
        str(out_path.parent.parent),
        out_text,
        err_text,
        condor_text,
        *card_texts,
    )

    events = parse_events_from_texts(out_text, err_text, *card_texts)

    started_backends = {m.group(1).lower() for m in BACKEND_START_RE.finditer(out_text)}
    finished_backends = {m.group(1).lower() for m in BACKEND_DONE_RE.finditer(out_text)}
    overall_all_done = "All backends (fortran, cppnone, cppavx2, cuda) completed!" in out_text

    mg_base_str = mg_base.group(1).strip() if mg_base else None

    created = condor.get("execute_started") or parse_dt_any(start.group(1) if start else None)

    rows: List[Dict[str, Any]] = []
    for i, backend in enumerate(BACKENDS):
        wall_s = benchmark_times[i] if i < len(benchmark_times) else None
        backend_started = backend in started_backends
        backend_finished = backend in finished_backends

        status = "unknown"
        fail_class = None

        if wall_s is not None and backend_finished:
            status = "ok"
        elif wall_s is not None and overall_all_done:
            status = "ok"
        elif backend_started and condor.get("condor_status") == "failed":
            status = "failed"
            fail_class = condor.get("condor_hold_reason") or "condor_job_failed"
        elif wall_s is not None:
            status = "ok"

        row: Dict[str, Any] = {
            "source": "standalone_mg_benchmark",
            "run_id": run_id,
            "cluster_id": cluster_id,
            "proc_id": proc_id,
            "csv_path": None,
            "job_report_path": None,
            "log_generate_path": str(out_path),
            "condor_log_path": str(condor_log_path) if condor_log_path else None,
            "stderr_path": str(err_path),
            "process": process,
            "jets": jets,
            "mode": mode or "unknown",
            "backend": backend,
            "madgraph_version": infer_mg_version_label(mg_base_str),
            "athena_version": None,
            "patch_shadow": False,
            "throughput": None,
            "me_evals_sec": None,
            "wall_s": wall_s,
            "events": events,
            "node": condor.get("node") or (host.group(1) if host else None),
            "cpu_name": condor.get("cpu_name"),
            "cpu_count": condor.get("cpu_count") or req_resources.get("request_cpus"),
            "gpu_name": condor.get("gpu_name") if backend == "cuda" else None,
            "gpu_count": condor.get("gpu_count") if backend == "cuda" else (req_resources.get("request_gpus") or 0),
            "mem_total_mb": condor.get("mem_request_mb"),
            "transform_exit_code": None,
            "transform_exit_msg": None,
            "generate_rc": None,
            "generate_status_ok": True if status == "ok" else False if status == "failed" else None,
            "csv_exitcode": 0 if status == "ok" else 1 if status == "failed" else None,
            "fail_class": fail_class,
            "status": status,
            "created": created,
            "raw_csv": {},
            "standalone_meta": {
                "job_start": parse_dt_any(start.group(1) if start else None),
                "job_end": parse_dt_any(end.group(1) if end else None),
                "host_reported": host.group(1) if host else None,
                "cuda_architecture": cuda_arch.group(1).strip() if cuda_arch else None,
                "python": python_v.group(1).strip() if python_v else None,
                "mg_base": mg_base_str,
                "cudacpp_plugin": plugin.group(1).strip() if plugin else None,
                "backend_started": backend_started,
                "backend_finished": backend_finished,
                "all_backends_completed": overall_all_done,
                "benchmark_real_index": 2 * (i + 1),
                "condor_status": condor.get("condor_status"),
                "condor_hold_reason": condor.get("condor_hold_reason"),
                "time_execute_s": condor.get("time_execute_s"),
                "max_memory_mb": condor.get("max_memory_mb"),
                "max_rss_kb": condor.get("max_rss_kb"),
                "slot": condor.get("slot"),
                "request_gpus": req_resources.get("request_gpus"),
                "request_cpus": req_resources.get("request_cpus"),
                "request_memory": req_resources.get("request_memory"),
                "card_paths": [str(p) for p in card_paths],
            },
        }
        rows.append(row)

    return rows


def scan_standalone(root: Path) -> List[Dict[str, Any]]:
    out_files = sorted(root.rglob("log-mg.*.*.out"))

    condor_logs: Dict[str, Path] = {}
    for p in root.rglob("log-mg.*.log"):
        m = CONDOR_LOG_RE.search(p.name)
        if m:
            condor_logs[m.group(1)] = p

    rows: List[Dict[str, Any]] = []
    for out_path in out_files:
        m = OUT_RE.search(out_path.name)
        if not m:
            continue

        cluster_id = m.group(1)
        proc_id = m.group(2)

        err_path = out_path.with_suffix(".err")
        if not err_path.is_file():
            alt = out_path.parent / f"log-mg.{cluster_id}.{proc_id}.err"
            if alt.is_file():
                err_path = alt
            else:
                continue

        rows.extend(build_rows_for_job(out_path, err_path, condor_logs.get(cluster_id)))

    rows.sort(key=lambda r: ((r.get("created") is None), str(r.get("created"))), reverse=True)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="Root of MG_standalone_lhe_shower")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    if not args.root.is_dir():
        print(json.dumps({"error": f"Not a directory: {args.root}"}), file=sys.stderr)
        return 2

    rows = scan_standalone(args.root)

    if args.pretty:
        print(json.dumps(rows, indent=2, sort_keys=False))
    else:
        print(json.dumps(rows, separators=(",", ":"), sort_keys=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())