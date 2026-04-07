#!/usr/bin/env python3

import json
import os
from pathlib import Path
from urllib.parse import parse_qs

DATA_FILE = Path("/eos/user/d/dbhandar/www/mgbench/data/manual_benchmarks.json")


def get_env_param(name: str) -> str:
    query = os.environ.get("QUERY_STRING", "")
    params = parse_qs(query)
    return params.get(name, [""])[0].strip()


def normalize_backend(row: dict) -> str:
    candidates = [
        row.get("backend"),
        row.get("raw_csv", {}).get("backend_label"),
        row.get("raw_csv", {}).get("backend_requested"),
        row.get("raw_csv", {}).get("backend_used"),
        row.get("raw_csv", {}).get("jo_dir"),
        row.get("raw_csv", {}).get("outtag"),
        row.get("csv_path"),
        row.get("log_generate_path"),
    ]
    text = " ".join(str(x) for x in candidates if x).lower()

    if "cpp512y" in text:
        return "cpp512y"
    if "cpp512z" in text:
        return "cpp512z"
    if "cppavx2" in text or " avx2" in text:
        return "cppavx2"
    if "cppnone" in text:
        return "cppnone"
    if "madevent_gpu" in text or "_gpu" in text or ".gpu." in text or " cuda" in text or text == "gpu":
        return "cuda"
    if "fortran" in text:
        return "fortran"

    value = row.get("backend")
    return str(value) if value not in (None, "") else "unknown"


def normalize_jets(row: dict) -> str:
    value = row.get("jets")
    if value not in (None, ""):
        return str(value)

    candidates = [
        row.get("raw_csv", {}).get("jo_dir"),
        row.get("raw_csv", {}).get("outtag"),
        row.get("csv_path"),
        row.get("log_generate_path"),
    ]
    text = " ".join(str(x) for x in candidates if x)

    import re
    m = re.search(r"_(\d+)(?:to(\d+))?J\b", text, re.IGNORECASE)
    if not m:
        return "unknown"
    if m.group(2):
        return str(m.group(2))   # 0to3J -> 3
    return str(m.group(1))


def normalize_patch(row: dict) -> str:
    return "shadow" if row.get("patch_shadow") else "none"


def normalize_combo(row: dict) -> str:
    mg = row.get("madgraph_version") or "unknown"
    ath = row.get("athena_version") or "unknown"
    patch = normalize_patch(row)
    return f"{mg} | {ath} | {patch}"


def matches_filters(row: dict, filters: dict) -> bool:
    process = str(row.get("process") or "unknown")
    backend = normalize_backend(row)
    jets = normalize_jets(row)
    mg = str(row.get("madgraph_version") or "unknown")
    athena = str(row.get("athena_version") or "unknown")
    patch = normalize_patch(row)
    combo = normalize_combo(row)

    if filters["process"] and process != filters["process"]:
        return False
    if filters["backend"] and backend != filters["backend"]:
        return False
    if filters["jets"] and jets != filters["jets"]:
        return False
    if filters["mg"] and mg != filters["mg"]:
        return False
    if filters["athena"] and athena != filters["athena"]:
        return False
    if filters["patch"] and patch != filters["patch"]:
        return False
    if filters["combo"] and combo != filters["combo"]:
        return False

    return True


print("Content-Type: application/json")
print()

if not DATA_FILE.is_file():
    print(json.dumps({"error": "cached data file not found"}))
    raise SystemExit

try:
    rows = json.loads(DATA_FILE.read_text(encoding="utf-8", errors="replace"))
except Exception as e:
    print(json.dumps({"error": f"failed to read cached data: {e}"}))
    raise SystemExit

if not isinstance(rows, list):
    print(json.dumps({"error": "cached data is not a JSON array"}))
    raise SystemExit

filters = {
    "process": get_env_param("process"),
    "backend": get_env_param("backend"),
    "jets": get_env_param("jets"),
    "mg": get_env_param("mg"),
    "athena": get_env_param("athena"),
    "patch": get_env_param("patch"),
    "combo": get_env_param("combo"),
}

filtered = [row for row in rows if matches_filters(row, filters)]
print(json.dumps(filtered, separators=(",", ":")))