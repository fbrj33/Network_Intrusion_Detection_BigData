#!/bin/bash
set -euo pipefail

hdfs dfs -mkdir -p /user/root /tmp/hadoop /data/raw/flows_batch
hdfs dfs -chmod -R 777 /tmp /user /data
echo "== Batch CSV to HDFS"
hdfs dfs -rm -r -f /data/raw/flows_batch
hdfs dfs -mkdir -p /data/raw/flows_batch
tail -n +2 /data/part_mysql.csv | hdfs dfs -put - /data/raw/flows_batch/part-m-00000
echo "== Flume"
pkill -f flume-ng 2>/dev/null || true
rm -rf /logs/sim_offset /logs/flowlogs /logs/flume_taildir_position.json
hdfs dfs -rm -r -f -skipTrash /data/raw/logs/flows
mkdir -p /logs/flowlogs && touch /logs/flowlogs/flows.log
nohup bash /flume/start_flume.sh > /logs/flume.log 2>&1 &
sleep 25
bash /flume/sim_logs.sh 20000 | tail -n 2
echo "flushing..."
sleep 60
hdfs dfs -ls /data/raw/flows_batch
hdfs dfs -ls -R /data/raw/logs/flows