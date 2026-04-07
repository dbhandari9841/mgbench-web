#!/usr/bin/bash
set -euo pipefail

REQDIR="/eos/user/d/dbhandar/www/mgbench/jobs/requests"
RESDIR="/eos/user/d/dbhandar/www/mgbench/jobs/results"
DONEDIR="${REQDIR}/done"
FAILEDDIR="${REQDIR}/failed"
LOCKFILE="/tmp/mgbench_process_requests.lock"
DEBUGLOG="/eos/user/d/dbhandar/www/mgbench/jobs/requests/process_requests_debug.log"

AFSROOT="/afs/cern.ch/user/d/dbhandar/mgbench"
SUBMIT_TEMPLATE="${AFSROOT}/condor/mgbench_one_backend.sub"
EXECUTABLE="${AFSROOT}/bin/run_mgbench_one_backend.sh"
CARDROOT="${AFSROOT}/cards"
LOGDIR="${AFSROOT}/logs"

json_escape() {
  python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

{
  echo "=================================================="
  echo "START $(date)"
  echo "USER=$(id -un 2>/dev/null || true)"
  echo "UID=$(id -u 2>/dev/null || true)"
  echo "PWD=$(pwd)"
  echo "REQDIR=$REQDIR"
  echo "RESDIR=$RESDIR"
  echo "DONEDIR=$DONEDIR"
  echo "FAILEDDIR=$FAILEDDIR"
  echo "AFSROOT=$AFSROOT"
  echo "SUBMIT_TEMPLATE=$SUBMIT_TEMPLATE"
  echo "EXECUTABLE=$EXECUTABLE"
  echo "CARDROOT=$CARDROOT"
  echo "LOGDIR=$LOGDIR"
} >> "$DEBUGLOG" 2>&1

for d in "$REQDIR" "$RESDIR" "$DONEDIR" "$FAILEDDIR" "$CARDROOT" "$LOGDIR"; do
  if [ ! -d "$d" ]; then
    echo "Missing required directory: $d" >> "$DEBUGLOG" 2>&1
    exit 1
  fi
done

for f in "$SUBMIT_TEMPLATE" "$EXECUTABLE"; do
  if [ ! -f "$f" ]; then
    echo "Missing required file: $f" >> "$DEBUGLOG" 2>&1
    exit 1
  fi
done

exec 9>"$LOCKFILE"
if ! /usr/bin/flock -n 9; then
  echo "Another processor instance already holds the lock" >> "$DEBUGLOG" 2>&1
  exit 0
fi

shopt -s nullglob
found_any=0

for req in "$REQDIR"/20*.txt; do
  [ -f "$req" ] || continue
  found_any=1

  echo "Considering request file: $req" >> "$DEBUGLOG" 2>&1

  base=$(basename "$req")
  if [[ ! "$base" =~ ^20[0-9]{6}T[0-9]{6}Z_[a-f0-9]{10}\.txt$ ]]; then
    echo "Skipping non-request file: $req" >> "$DEBUGLOG" 2>&1
    continue
  fi

  request_id=""
  process=""
  jets=""
  mode=""
  version=""
  backend=""
  nevt=""
  card_key=""

  if ! source "$req"; then
    echo "Failed to parse request: $req" >> "$DEBUGLOG" 2>&1
    mv "$req" "$FAILEDDIR"/
    continue
  fi

  echo "Parsed request_id=$request_id process=$process jets=$jets mode=$mode version=$version backend=$backend nevt=$nevt card_key=$card_key" >> "$DEBUGLOG" 2>&1

  if [[ -z "${request_id:-}" ]]; then
    echo "Missing request_id in: $req" >> "$DEBUGLOG" 2>&1
    mv "$req" "$FAILEDDIR"/
    continue
  fi

  if [[ -z "${nevt:-}" || ! "$nevt" =~ ^[0-9]+$ ]]; then
    echo "Invalid nevt in: $req" >> "$DEBUGLOG" 2>&1
    mv "$req" "$FAILEDDIR"/
    continue
  fi

  status_file="$RESDIR/${request_id}.status.json"
  result_file="$RESDIR/${request_id}.result.json"

  cat > "$status_file" <<EOF
{
  "request_id": "$request_id",
  "state": "submitting",
  "process": "$process",
  "jets": "$jets",
  "mode": "$mode",
  "version": "$version",
  "backend": "$backend",
  "nevt": $nevt,
  "card_key": "$card_key"
}
EOF

  case "$backend" in
    fortran)
      request_gpus="0"
      request_cpus="8"
      request_memory="16GB"
      requirements='(OpSysAndVer =?= "AlmaLinux9")'
      ;;
    cppnone)
      request_gpus="0"
      request_cpus="8"
      request_memory="16GB"
      requirements='(OpSysAndVer =?= "AlmaLinux9")'
      ;;
    cppavx2)
      request_gpus="0"
      request_cpus="8"
      request_memory="16GB"
      requirements='(OpSysAndVer =?= "AlmaLinux9")'
      ;;
    cuda)
      request_gpus="1"
      request_cpus="8"
      request_memory="16GB"
      requirements='(GPUs > 0) && (GPUs_DeviceName =!= undefined) && regexp("A100", GPUs_DeviceName)'
      ;;
    *)
      err="Unsupported backend: $backend"
      echo "$err" >> "$DEBUGLOG" 2>&1
      err_json=$(printf '%s' "$err" | json_escape)
      cat > "$status_file" <<EOF
{
  "request_id": "$request_id",
  "state": "submit_failed",
  "error": $err_json
}
EOF
      mv "$req" "$FAILEDDIR"/
      continue
      ;;
  esac

  card_a="${CARDROOT}/${version}/${card_key}/input_card_a_${backend}.txt"
  card_b="${CARDROOT}/${version}/${card_key}/input_card_b_${backend}.txt"

  for f in "$card_a" "$card_b"; do
    if [ ! -f "$f" ]; then
      err="Missing required input file: $f"
      echo "$err" >> "$DEBUGLOG" 2>&1
      err_json=$(printf '%s' "$err" | json_escape)
      cat > "$status_file" <<EOF
{
  "request_id": "$request_id",
  "state": "submit_failed",
  "error": $err_json
}
EOF
      mv "$req" "$FAILEDDIR"/
      continue 2
    fi
  done

  card_a_basename=$(basename "$card_a")
  card_b_basename=$(basename "$card_b")
  transfer_input_files="${EXECUTABLE},${card_a},${card_b}"

  echo "Submitting request_id=$request_id backend=$backend" >> "$DEBUGLOG" 2>&1
  echo "card_a=$card_a" >> "$DEBUGLOG" 2>&1
  echo "card_b=$card_b" >> "$DEBUGLOG" 2>&1
  echo "CARD_A_BASENAME=$card_a_basename" >> "$DEBUGLOG" 2>&1
  echo "CARD_B_BASENAME=$card_b_basename" >> "$DEBUGLOG" 2>&1
  echo "transfer_input_files=$transfer_input_files" >> "$DEBUGLOG" 2>&1
  echo "----- submit template begin -----" >> "$DEBUGLOG" 2>&1
  cat "$SUBMIT_TEMPLATE" >> "$DEBUGLOG" 2>&1
  echo "----- submit template end -----" >> "$DEBUGLOG" 2>&1

  submit_output=$(
    cd "$AFSROOT" && \
    /usr/bin/condor_submit -verbose \
      EXECUTABLE="$EXECUTABLE" \
      CARD_A_BASENAME="$card_a_basename" \
      CARD_B_BASENAME="$card_b_basename" \
      REQID="$request_id" \
      LOGDIR="$LOGDIR" \
      TRANSFER_INPUT_FILES="$transfer_input_files" \
      REQUEST_GPUS="$request_gpus" \
      REQUEST_CPUS="$request_cpus" \
      REQUEST_MEMORY="$request_memory" \
      REQUIREMENTS="$requirements" \
      "$SUBMIT_TEMPLATE" 2>&1
  ) || {
    echo "condor_submit failed for $request_id" >> "$DEBUGLOG" 2>&1
    echo "$submit_output" >> "$DEBUGLOG" 2>&1
    err_json=$(printf '%s' "$submit_output" | json_escape)
    cat > "$status_file" <<EOF
{
  "request_id": "$request_id",
  "state": "submit_failed",
  "error": $err_json
}
EOF
    mv "$req" "$FAILEDDIR"/
    continue
  }

  echo "$submit_output" >> "$DEBUGLOG" 2>&1

  cluster_id=$(
    printf '%s\n' "$submit_output" | python3 -c '
import re, sys
text = sys.stdin.read()
patterns = [
    r"ClusterId\s*=\s*([0-9]+)",
    r"\*\*\s+Proc\s+([0-9]+)\.[0-9]+:",
    r"cluster\s+([0-9]+)"
]
for pat in patterns:
    m = re.search(pat, text, re.IGNORECASE)
    if m:
        print(m.group(1))
        break
'
  )

  if [[ -z "${cluster_id:-}" ]]; then
    err="condor_submit succeeded but cluster id could not be parsed"
    echo "$err" >> "$DEBUGLOG" 2>&1
    err_json=$(printf '%s\n\n%s' "$err" "$submit_output" | json_escape)
    cat > "$status_file" <<EOF
{
  "request_id": "$request_id",
  "state": "submit_failed",
  "error": $err_json
}
EOF
    mv "$req" "$FAILEDDIR"/
    continue
  fi

  cat > "$status_file" <<EOF
{
  "request_id": "$request_id",
  "state": "submitted",
  "cluster_id": "$cluster_id",
  "process": "$process",
  "jets": "$jets",
  "mode": "$mode",
  "version": "$version",
  "backend": "$backend",
  "nevt": $nevt,
  "card_key": "$card_key",
  "card_a": "$card_a_basename",
  "card_b": "$card_b_basename",
  "timed_phase": "card_b"
}
EOF

  cat > "$result_file" <<EOF
{
  "request_id": "$request_id",
  "state": "submitted",
  "cluster_id": "$cluster_id",
  "timed_phase": "card_b"
}
EOF

  echo "Submitted cluster_id=$cluster_id for request_id=$request_id" >> "$DEBUGLOG" 2>&1
  mv "$req" "$DONEDIR"/
done

if [ "$found_any" -eq 0 ]; then
  echo "No request files found" >> "$DEBUGLOG" 2>&1
fi

echo "END $(date)" >> "$DEBUGLOG" 2>&1