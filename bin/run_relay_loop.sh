#!/usr/bin/bash
set -euo pipefail

AFSROOT="/afs/cern.ch/user/d/dbhandar/mgbench"
PROCESSOR="/eos/user/d/dbhandar/www/mgbench/bin/process_requests.sh"
LOG="/eos/user/d/dbhandar/www/mgbench/jobs/requests/relay.log"

cd "$AFSROOT"
touch "$LOG"

echo "==================================================" >> "$LOG"
echo "Relay started at $(date)" >> "$LOG"
echo "Running as user $(id -un) uid $(id -u)" >> "$LOG"
echo "PWD=$(pwd)" >> "$LOG"

while true; do
  echo "--- relay tick $(date) ---" >> "$LOG"
  /usr/bin/bash "$PROCESSOR" >> "$LOG" 2>&1 || true
  sleep 5
done