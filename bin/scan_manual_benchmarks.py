#!/usr/bin/env python3
"""
Scan MANUAL_BENCHMARK_PATCHED and emit normalized benchmark JSON.

Usage:
    python3 scan_manual_benchmarks.py \
        /afs/cern.ch/user/d/dbhandar/MANUAL_BENCHMARK_PATCHED \
        > manual_benchmarks.json

Optional:
    python3 scan_manual_benchmarks.py /path/to/root --pretty
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import signal
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Avoid noisy BrokenPipeError when piping into head
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

RUN_ID_RE = re.compile(r"(\d+\.\d+)(?=\D*$)")
MG_VERSION_RE = re.compile(r"VERSION\s+([0-9]+\.[0-9]+\.[0-9]+)", re.IGNORECASE)
ATHENA_VERSION_RE = re.compile(r"AthGeneration[, \-]*([0-9]+\.[0-9]+\.[0-9]+)", re.IGNORECASE)
PROCESS_RE = re.compile(r"\b(DY|TT)\b", re.IGNORECASE)

# More flexible jet parsing patterns.
# Examples matched:
#   DY_0J_fortran
#   TT_0to2J_gpu
#   DY_3J_cppavx2
#   DY_0to3j
#   TT 0 to 2 j
#   /some/path/DY_4j_cuda/
JET_PATTERNS = [
    re.compile(r"(?:^|[^0-9])(\d+)\s*to\s*(\d+)\s*j(?:[^a-z0-9]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[^0-9])(\d+)to(\d+)j(?:[^a-z0-9]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[^0-9])(\d+)j(?:[^a-z0-9]|$)", re.IGNORECASE),
    re.compile(r"_(\d+)(?:to(\d+))?J\b", re.IGNORECASE),
]

# Example in logs:
# Building madevent in madevent_interface.py with 'fortran' matrix elements
LOG_MATRIX_BACKEND_RE = re.compile(r"using\s+'([^']+)'\s+matrix elements", re.IGNORECASE)
LOG_MADEVENT_BACKEND_RE = re.compile(
    r"Building madevent .* with '([^']+)' matrix elements", re.IGNORECASE
)

BACKEND_ORDER = [
    "cuda",
    "cpp512y",
    "cpp512z",
    "cppavx2",
    "cppnone",
    "fortran",
]

BACKEND_PATTERNS = {
    "cuda": [
        "madevent_gpu",
        "backend_requested=madevent_gpu",
        "madgraph_devices=madevent_gpu",
        "_gpu",
        ".gpu.",
        " gpu ",
        "cuda",
    ],
    "cpp512y": [
        "cpp512y",
    ],
    "cpp512z": [
        "cpp512z",
    ],
    "cppavx2": [
        "cppavx2",
        "avx2",
    ],
    "cppnone": [
        "cppnone",
    ],
    "fortran": [
        "fortran",
    ],
}


def read_text_safe(path: Path, max_bytes: int = 5_000_000) -> str:
    try:
        data = path.read_bytes()
        if len(data) > max_bytes:
            data = data[:max_bytes]
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def read_json_safe(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            return json.load(f)
    except Exception:
        return None


def normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", key.strip().lower()).strip("_")


def first_present(dct: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in dct and dct[k] not in ("", None):
            return dct[k]
    return None


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def extract_run_id(path: Path) -> Optional[str]:
    m = RUN_ID_RE.search(path.name)
    return m.group(1) if m else None


def find_files(root: Path, pattern_prefixes: Iterable[str]) -> List[Path]:
    out: List[Path] = []
    prefixes = tuple(pattern_prefixes)
    for p in root.rglob("*"):
        if p.is_file() and p.name.startswith(prefixes):
            out.append(p)
    return out


def index_by_run_id(paths: Iterable[Path]) -> Dict[str, List[Path]]:
    idx: Dict[str, List[Path]] = {}
    for p in paths:
        run_id = extract_run_id(p)
        if run_id:
            idx.setdefault(run_id, []).append(p)
    return idx


def choose_best(paths: List[Path], prefer_suffix: Optional[str] = None) -> Optional[Path]:
    if not paths:
        return None
    if prefer_suffix:
        preferred = [p for p in paths if p.name.endswith(prefer_suffix)]
        if preferred:
            return sorted(preferred)[0]
    return sorted(paths)[0]


def parse_csv_rows(csv_path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                row = {normalize_key(k): v for k, v in raw.items() if k is not None}
                rows.append(row)
    except Exception as exc:
        rows.append({"_csv_parse_error": str(exc)})
    return rows


def parse_jets_and_mode_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None

    for pat in JET_PATTERNS:
        m = pat.search(text)
        if not m:
            continue

        low = m.group(1)
        high = m.group(2) if m.lastindex and m.lastindex >= 2 else None

        # 0to3J / 0to3j / 0 to 3 j -> jets=3, inclusive
        if high is not None:
            return str(high), "inclusive"

        # 3J / 3j -> jets=3, exclusive by default
        return str(low), "exclusive"

    return None, None


def canonicalize_backend(backend: Optional[str]) -> Optional[str]:
    if backend is None:
        return None

    b = str(backend).strip().lower()
    if not b:
        return None

    if b == "gpu":
        return "cuda"

    if b in {"cuda", "cpp512y", "cpp512z", "cppavx2", "cppnone", "fortran"}:
        return b

    if "cpp512y" in b:
        return "cpp512y"
    if "cpp512z" in b:
        return "cpp512z"
    if "cppavx2" in b or b == "avx2":
        return "cppavx2"
    if "cppnone" in b:
        return "cppnone"
    if "fortran" in b:
        return "fortran"
    if "madevent_gpu" in b or "cuda" in b:
        return "cuda"
    if "gpu" in b:
        return "cuda"

    return b


def infer_backend_from_text(text: str) -> Optional[str]:
    t = text.lower()

    for backend in BACKEND_ORDER:
        for pattern in BACKEND_PATTERNS[backend]:
            if pattern in t:
                return backend

    return None


def infer_backend_from_log(log_text: str) -> Optional[str]:
    for regex in (LOG_MATRIX_BACKEND_RE, LOG_MADEVENT_BACKEND_RE):
        m = regex.search(log_text)
        if m:
            return canonicalize_backend(m.group(1))
    return None


def infer_process_jets_mode_backend(
    csv_row: Dict[str, Any],
    csv_path: Path,
    log_text: str,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    process = first_present(csv_row, "process", "proc")
    jets = first_present(
        csv_row,
        "jets",
        "njet",
        "njets",
        "jet_mult",
        "jet_multiplicity",
    )
    mode = first_present(csv_row, "mode")
    backend = first_present(
        csv_row,
        "backend",
        "backend_seen",
        "backend_used",
        "backend_label",
        "backend_requested",
        "cpu_backend",
        "device",
        "devices",
    )

    haystacks = [
        csv_path.name,
        str(csv_path),
        str(first_present(
            csv_row,
            "jo_dir",
            "outtag",
            "sample",
            "job_name",
            "jobconfig",
            "process",
            "proc",
        ) or ""),
        str(first_present(csv_row, "backend_label", "backend_requested", "backend_used") or ""),
    ]

    if process:
        process = str(process).upper()

    if jets is not None:
        jets = str(jets).strip()

    if mode is not None:
        mode = str(mode).lower()

    backend = canonicalize_backend(backend)

    # If jets is present but not normalized, try to interpret it.
    if jets:
        parsed_jets, parsed_mode = parse_jets_and_mode_from_text(jets)
        if parsed_jets is not None:
            jets = parsed_jets
        if mode is None and parsed_mode is not None:
            mode = parsed_mode

    for text in haystacks:
        if process is None:
            pm = PROCESS_RE.search(text)
            if pm:
                process = pm.group(1).upper()

        if jets is None or mode is None:
            parsed_jets, parsed_mode = parse_jets_and_mode_from_text(text)
            if jets is None and parsed_jets is not None:
                jets = parsed_jets
            if mode is None and parsed_mode is not None:
                mode = parsed_mode

        if backend is None or backend == "unknown":
            inferred = infer_backend_from_text(text)
            if inferred:
                backend = inferred

    if backend is None or backend == "unknown":
        inferred_log_backend = infer_backend_from_log(log_text)
        if inferred_log_backend:
            backend = inferred_log_backend

    combined_text = " ".join(haystacks).lower()
    if mode is None:
        if "inclusive" in combined_text or "0to" in combined_text:
            mode = "inclusive"
        elif "exclusive" in combined_text:
            mode = "exclusive"

    if process:
        process = str(process).upper()
    if jets is not None:
        jets = str(jets)
    if mode is not None:
        mode = str(mode).lower()
    if backend is not None:
        backend = canonicalize_backend(backend)

    return process, jets, mode, backend


def extract_versions_and_patch(
    log_text: str, job_report: Optional[Dict[str, Any]]
) -> Tuple[Optional[str], Optional[str], bool]:
    mg_version = None
    athena_version = None
    has_shadow_patch = False

    mg_match = MG_VERSION_RE.search(log_text)
    if mg_match:
        mg_version = mg_match.group(1)

    ath_match = ATHENA_VERSION_RE.search(log_text)
    if ath_match:
        athena_version = ath_match.group(1)

    if job_report:
        cmdline = str(job_report.get("cmdLine", ""))
        if athena_version is None:
            ath_match = ATHENA_VERSION_RE.search(cmdline)
            if ath_match:
                athena_version = ath_match.group(1)

    if "shadow" in log_text.lower():
        has_shadow_patch = True

    return mg_version, athena_version, has_shadow_patch


def extract_hw(job_report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "node": None,
        "cpu_name": None,
        "cpu_count": None,
        "gpu_name": None,
        "gpu_count": None,
        "mem_total_mb": None,
    }
    if not job_report:
        return out

    machine = job_report.get("resource", {}).get("machine", {})
    out["node"] = machine.get("node")
    out["cpu_name"] = machine.get("model_name")

    hw = (
        job_report.get("resource", {})
        .get("executor", {})
        .get("generate", {})
        .get("memory", {})
        .get("HW", {})
    )

    cpu = hw.get("cpu", {})
    gpu = hw.get("gpu", {})
    mem = hw.get("mem", {})

    out["cpu_name"] = out["cpu_name"] or cpu.get("ModelName")
    out["cpu_count"] = cpu.get("CPUs")
    out["mem_total_mb"] = (
        int(mem["MemTotal"]) // 1024 if isinstance(mem.get("MemTotal"), (int, float)) else None
    )

    gpu_names: List[str] = []
    for key, val in gpu.items():
        if key == "nGPU":
            continue
        if isinstance(val, dict) and "name" in val:
            gpu_names.append(str(val["name"]))
    out["gpu_name"] = gpu_names[0] if gpu_names else None
    out["gpu_count"] = gpu.get("nGPU")

    if not out["gpu_name"]:
        flat = json.dumps(job_report)
        gm = re.search(r"NVIDIA [A-Za-z0-9\- ]+", flat)
        if gm:
            out["gpu_name"] = gm.group(0).strip()

    return out


def extract_status(job_report: Optional[Dict[str, Any]], csv_row: Dict[str, Any]) -> Dict[str, Any]:
    csv_exitcode = to_int(first_present(csv_row, "exitcode", "exit_code"))

    out = {
        "transform_exit_code": None,
        "transform_exit_msg": None,
        "generate_rc": None,
        "generate_status_ok": None,
        "csv_exitcode": csv_exitcode,
        "fail_class": first_present(csv_row, "fail_class"),
        "status": "unknown",
    }

    if job_report:
        out["transform_exit_code"] = job_report.get("exitCode")
        out["transform_exit_msg"] = job_report.get("exitMsg")

        executors = job_report.get("executor", [])
        if isinstance(executors, list):
            for ex in executors:
                if ex.get("name") == "generate":
                    out["generate_rc"] = ex.get("rc")
                    out["generate_status_ok"] = ex.get("statusOK")
                    break

    if out["csv_exitcode"] is not None:
        out["status"] = "ok" if out["csv_exitcode"] == 0 else "failed"
    elif out["transform_exit_code"] is not None:
        out["status"] = "ok" if out["transform_exit_code"] == 0 else "failed"
    elif out["generate_status_ok"] is not None:
        out["status"] = "ok" if out["generate_status_ok"] else "failed"

    return out


def extract_metrics(csv_row: Dict[str, Any], job_report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    throughput = first_present(
        csv_row,
        "throughput",
        "evt_per_s_generate",
        "evt_per_s_shell",
        "evt_per_s",
        "ev_per_s",
        "events_per_second",
        "event_throughput",
    )
    me_eval_s = first_present(
        csv_row,
        "me_evals_sec",
        "me_eval_s",
        "matrix_element_evals_per_second",
        "me_per_s",
    )
    wall_s = first_present(
        csv_row,
        "wall_s",
        "shell_wall_s",
        "generate_wall_s",
        "generate_total_wall_s",
        "transform_wall_s",
    )
    events = first_present(
        csv_row,
        "nevt_done",
        "nevt_req",
        "nevt",
        "events",
        "maxevents",
        "requested_events",
    )

    if wall_s is None and job_report:
        wall_s = (
            job_report.get("resource", {})
            .get("executor", {})
            .get("generate", {})
            .get("wallTime")
        )

    return {
        "throughput": to_float(throughput),
        "me_evals_sec": to_float(me_eval_s),
        "wall_s": to_float(wall_s),
        "events": to_int(events),
    }


def normalize_row(
    csv_path: Path,
    csv_row: Dict[str, Any],
    run_id: str,
    job_report_path: Optional[Path],
    log_generate_path: Optional[Path],
) -> Dict[str, Any]:
    job_report = read_json_safe(job_report_path) if job_report_path else None
    log_text = read_text_safe(log_generate_path) if log_generate_path else ""

    process, jets, mode, backend = infer_process_jets_mode_backend(csv_row, csv_path, log_text)
    mg_version, athena_version, has_shadow_patch = extract_versions_and_patch(log_text, job_report)
    hw = extract_hw(job_report)
    status = extract_status(job_report, csv_row)
    metrics = extract_metrics(csv_row, job_report)

    row: Dict[str, Any] = {
        "source": "manual_benchmark_patched",
        "run_id": run_id,
        "csv_path": str(csv_path),
        "job_report_path": str(job_report_path) if job_report_path else None,
        "log_generate_path": str(log_generate_path) if log_generate_path else None,
        "process": process,
        "jets": jets,
        "mode": mode,
        "backend": backend,
        "madgraph_version": mg_version,
        "athena_version": athena_version,
        "patch_shadow": has_shadow_patch,
        **metrics,
        **hw,
        **status,
        "created": None,
        "raw_csv": csv_row,
    }

    if job_report:
        row["created"] = job_report.get("created")

    if row["created"] is None:
        try:
            row["created"] = csv_path.stat().st_mtime
        except Exception:
            pass

    return row


def scan_manual_benchmarks(root: Path) -> List[Dict[str, Any]]:
    csv_files = find_files(root, ["bench_row"])
    job_report_files = find_files(root, ["jobReport"])
    log_generate_files = find_files(root, ["log.generate"])

    job_report_idx = index_by_run_id(job_report_files)
    log_generate_idx = index_by_run_id(log_generate_files)

    all_rows: List[Dict[str, Any]] = []

    for csv_path in sorted(csv_files):
        run_id = extract_run_id(csv_path)
        if not run_id:
            continue

        job_report_path = choose_best(job_report_idx.get(run_id, []), prefer_suffix=".json")
        log_generate_path = choose_best(log_generate_idx.get(run_id, []))

        for csv_row in parse_csv_rows(csv_path):
            row = normalize_row(
                csv_path=csv_path,
                csv_row=csv_row,
                run_id=run_id,
                job_report_path=job_report_path,
                log_generate_path=log_generate_path,
            )
            all_rows.append(row)

    def sort_key(r: Dict[str, Any]) -> Tuple[int, str]:
        created = r.get("created")
        if isinstance(created, str):
            return (0, created)
        if created is None:
            return (1, "")
        return (0, str(created))

    all_rows.sort(key=sort_key, reverse=True)
    return all_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path, help="Root of MANUAL_BENCHMARK_PATCHED")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    args = parser.parse_args()

    if not args.root.is_dir():
        print(json.dumps({"error": f"Not a directory: {args.root}"}), file=sys.stderr)
        return 2

    rows = scan_manual_benchmarks(args.root)

    if args.pretty:
        print(json.dumps(rows, indent=2, sort_keys=False))
    else:
        print(json.dumps(rows, separators=(",", ":"), sort_keys=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())