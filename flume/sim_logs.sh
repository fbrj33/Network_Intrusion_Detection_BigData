#!/bin/bash
# Usage : bash sim_logs.sh LIGNES_PAR_SECONDE [MAX_LIGNES]
RATE=${1:-500}
MAX=${2:-0}
SRC=/data/part_logs.csv
OUT=/logs/flowlogs/flows.log
OFFSET_FILE=/logs/sim_offset
mkdir -p /logs/flowlogs
START=$(cat "$OFFSET_FILE" 2>/dev/null || echo 0)
exec 3< <(tail -n +$((START + 2)) "$SRC")
n=0
while mapfile -t -n "$RATE" -u 3 lines && [ "${#lines[@]}" -gt 0 ]; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  lines=("${lines[@]%$'\r'}")
  printf "$ts,%s\n" "${lines[@]}" >> "$OUT"
  n=$((n + ${#lines[@]}))
  echo $((START + n)) > "$OFFSET_FILE"
  echo "$n lignes ecrites"
  if [ "$MAX" -gt 0 ] && [ "$n" -ge "$MAX" ]; then break; fi
  sleep 1
done
echo "Termine : $n lignes (total $((START + n)))"