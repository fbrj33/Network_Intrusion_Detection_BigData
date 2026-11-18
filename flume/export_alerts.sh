#!/bin/bash
set -euo pipefail

hdfs dfs -test -e /data/results
codegen_dir=/tmp/sqoop-alerts-codegen
mkdir -p "$codegen_dir/src" "$codegen_dir/classes"
sqoop codegen \
  --connect 'jdbc:mysql://mysql:3306/telecom?useSSL=false&allowPublicKeyRetrieval=true' \
  --username root \
  --password root \
  --table alerts \
  --class-name alerts \
  --outdir "$codegen_dir/src" \
  --bindir "$codegen_dir/classes"

sqoop export \
  -libjars "$codegen_dir/classes/alerts.jar" \
  --connect 'jdbc:mysql://mysql:3306/telecom?useSSL=false&allowPublicKeyRetrieval=true' \
  --username root \
  --password root \
  --table alerts \
  --export-dir /data/results \
  --input-fields-terminated-by ',' \
  --input-lines-terminated-by '\n' \
  --num-mappers 1 \
  --update-key flow_id \
  --update-mode allowinsert
