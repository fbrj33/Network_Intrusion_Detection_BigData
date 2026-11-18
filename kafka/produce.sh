#!/bin/bash
set -euo pipefail

timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
awk -v ts="$timestamp" 'NR > 1 { print ts "," $0 }' /data/part_stream.csv |
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic network_flows
