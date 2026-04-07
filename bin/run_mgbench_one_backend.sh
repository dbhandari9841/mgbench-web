#!/usr/bin/bash
set -euo pipefail

CARD_A="$1"
CARD_B="$2"
REQID="$3"

RESULTS_DIR="/eos/user/d/dbhandar/www/mgbench/jobs/results"

STATUS_FILE="${RESULTS_DIR}/${REQID}.status.json"
RESULT_FILE="${RESULTS_DIR}/${REQID}.result.json"
RUNLOG_FILE="${RESULTS_DIR}/${REQID}.runlog.txt"

trap 'cat > "$STATUS_FILE" <<EOF
{
  "request_id": "'"$REQID"'",
  "state": "run_failed",
  "card_a": "'"$CARD_A"'",
  "card_b": "'"$CARD_B"'",
  "timed_phase": "card_b",
  "host": "'"$(hostname)"'"
}
EOF' ERR

{
  echo "REQID=$REQID"
  echo "CARD_A=$CARD_A"
  echo "CARD_B=$CARD_B"
  echo "HOST=$(hostname)"
  echo "START=$(date)"
  echo "PWD=$(pwd)"
  echo "FILES IN SCRATCH:"
  ls -l
} > "$RUNLOG_FILE" 2>&1

cat > "$STATUS_FILE" <<EOF
{
  "request_id": "$REQID",
  "state": "running",
  "card_a": "$CARD_A",
  "card_b": "$CARD_B",
  "host": "$(hostname)"
}
EOF

echo "PHASE_A_SETUP_START=$(date)" >> "$RUNLOG_FILE"

# Phase A: setup / export / preparation (not timed)
sleep 2

echo "PHASE_B_TIMED_START=$(date)" >> "$RUNLOG_FILE"

# Phase B: actual timed launch/run using input_card_b
start_ts=$(date +%s)

sleep 20

end_ts=$(date +%s)
wall_s=$((end_ts - start_ts))

cat >> "$RUNLOG_FILE" <<EOF
TIMED_PHASE_START=$start_ts
TIMED_PHASE_END=$end_ts
TIMED_PHASE_WALL_S=$wall_s
EOF

cat > "$RESULT_FILE" <<EOF
{
  "request_id": "$REQID",
  "state": "done",
  "card_a": "$CARD_A",
  "card_b": "$CARD_B",
  "timed_phase": "card_b",
  "wall_s": $wall_s,
  "host": "$(hostname)"
}
EOF

cat > "$STATUS_FILE" <<EOF
{
  "request_id": "$REQID",
  "state": "done",
  "card_a": "$CARD_A",
  "card_b": "$CARD_B",
  "timed_phase": "card_b",
  "wall_s": $wall_s,
  "host": "$(hostname)"
}
EOF