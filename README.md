# CIC-IDS-2017 Network Intrusion Detection

This project uses Hadoop HDFS, Hive, Spark MLlib, Kafka, Sqoop, and MySQL to
train and run a network-flow intrusion detector.

## Project status

The main batch and streaming pipeline has been run end to end:

- Hive external tables were created for both HDFS inputs and the analytics
  queries ran successfully.
- Spark trained a class-weighted Random Forest and saved the model to HDFS.
- Structured Streaming read timestamped CSV records from Kafka, applied the
  saved model, and wrote predictions to Kafka and HDFS.
- The final producer run sent 771,788 flows. Kafka alerts, HDFS results, and
  MySQL `alerts` each contain 771,788 records.
- Persistent HDFS, Kafka, and Hive metastore volumes are configured.
- The latest cleanup check found no `usage_events` Kafka topic or
  `customers`/`churn_scores` MySQL tables.

The report, screenshots, and formal reconciliation with the team's data
contract are still outstanding. Grafana is available in Compose but has not
been verified as part of the final report.

## Architecture

```text
Prepared CSV files
  ├─ Batch flows (B) ───────────────> HDFS /data/raw/flows_batch
  └─ Flume log flows (A) ───────────> HDFS /data/raw/logs/flows
                                        ├─ Hive external tables and analytics
                                        └─ Spark MLlib training
                                             └─ HDFS /models/rf_ids

Kafka network_flows
  └─ Spark Structured Streaming + saved model
       ├─ Kafka alerts
       └─ HDFS /data/results
             └─ Sqoop export ───────> MySQL telecom.alerts
```

## Verified inputs and results

### Hive and HDFS

The current HDFS input counts were:

| Input | HDFS path | Rows |
|---|---|---:|
| Batch/MySQL flows (B) | `/data/raw/flows_batch` | 257,265 |
| Flume log flows (A) | `/data/raw/logs/flows` | 1,543,587 |

Hive tables `flows_batch` and `flows_logs` point to those paths. The SQL file
`hive/queries.sql` includes row counts, attack distributions, destination-port
rankings, traffic statistics, and log volume by hour.

**Schema limitation:** the prepared input does not contain a `protocol`
column. Hive therefore reports destination-port analytics instead of claiming
to report protocol distributions.

### Spark MLlib

`spark/train.py` reads both HDFS sources (1,800,852 rows total), downsamples the
dominant BENIGN class, applies class weights, evaluates weighted and per-class
precision/recall/F1, and saves:

- `hdfs://namenode:8020/models/rf_ids`
- `hdfs://namenode:8020/models/label_indexer`

The current run used 20 trees, max depth 10, and 32 maximum bins. Weighted
accuracy was 0.9939, weighted precision 0.9939, weighted recall 0.9939, and
weighted F1 0.9938. These aggregate metrics hide weak performance on some
very-rare labels: Web Attack XSS and SQL Injection had F1 of 0 on the test
split. Review per-class metrics in `spark/train_result.log` before treating
this model as production-ready.

### Streaming, Kafka, and Sqoop

`spark/stream.py` parses a timestamp followed by the flow CSV columns, loads
the saved model, and writes these fields to HDFS `/data/results`:

```text
flow_id,event_ts,source_day,predicted_label,actual_label,confidence
```

It also writes JSON predictions to Kafka `alerts`. Both streams use
checkpoints under `/checkpoints`.

The verified final run processed 771,788 records:

- Kafka `network_flows`: 771,788 input messages.
- Kafka `alerts`: 771,788 output messages.
- HDFS `/data/results`: 771,788 rows.
- MySQL `telecom.alerts`: 771,788 rows with 771,788 unique flow IDs.

The topic was empty before the final producer run. It is now populated; do not
rerun the full producer unless you intend to enqueue the dataset again.

## Storage and local data

Compose uses persistent Docker volumes for the NameNode, DataNode, Kafka, and
Hive metastore. MySQL and Grafana also have data volumes. The client mounts
`data/prepared`, `flume`, and `logs`; the Kafka service mounts the producer
script and prepared stream CSV.

The HDFS, Kafka, and Hive volumes are declared as external volumes. On a new
machine, create them once before starting Compose:

```powershell
docker volume create network_intrusion_detection_bigdata_namenode_data
docker volume create network_intrusion_detection_bigdata_datanode_data
docker volume create network_intrusion_detection_bigdata_kafka_data
docker volume create network_intrusion_detection_bigdata_hive_metastore
```

The source datasets are local and are excluded from Git. Put the prepared
files in `data/prepared` before using the producer or rebuilding the HDFS
inputs. If the raw HDFS inputs are missing, the loader can repopulate them
from the local prepared files. It replaces the existing raw HDFS input paths,
so run it only when that reset is intended:

```powershell
docker compose exec client bash /flume/load.sh
```

Do not remove Docker volumes if you need to preserve the current HDFS, Kafka,
Hive, or MySQL state.

## Run the services

From the repository root in PowerShell:

```powershell
docker compose up -d --build
docker compose ps
```

HDFS is available at <http://localhost:9870>. The Compose stack includes
NameNode, DataNode, Hive, Spark, Kafka, MySQL, the Sqoop/Flume client, and
Grafana.

### Re-run Hive analytics

```powershell
docker compose exec hive beeline -u jdbc:hive2://localhost:10000 -f /hive/tables.sql
docker compose exec hive beeline -u jdbc:hive2://localhost:10000 -f /hive/queries.sql
```

### Re-train the model

```powershell
docker compose exec spark /opt/spark/bin/spark-submit --master "local[3]" /app/train.py
```

### Run streaming, then produce flows

Start streaming in one terminal and leave it running:

```powershell
docker compose exec spark /opt/spark/bin/spark-submit --master "local[3]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /app/stream.py
```

In a second terminal, send the prepared dataset:

```powershell
docker compose exec kafka bash /app/produce.sh
```

Streaming starts from the latest Kafka offsets and resumes from its HDFS
checkpoints. A fresh full replay requires a deliberate reset of the input
topic, stream checkpoints, HDFS results, and MySQL export target; do not delete
those paths or volumes casually.

### Export predictions to MySQL

```powershell
docker compose exec client bash /flume/export_alerts.sh
docker compose exec mysql mysql -uroot -proot telecom -e "SELECT COUNT(*) FROM alerts;"
```

The export script generates and adds the Sqoop record JAR to the MapReduce
classpath before exporting. This avoids the `ClassNotFoundException` seen
during the initial export attempts.



## Stop services

```powershell
docker compose stop
```

This stops containers without deleting the data volumes.
