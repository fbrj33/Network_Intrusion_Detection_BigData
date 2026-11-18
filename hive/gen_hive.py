import csv, os

cols = next(csv.reader(open("data/prepared/part_mysql.csv", newline="")))
has = lambda c: c in cols

# columns.txt for Spark
os.makedirs("spark", exist_ok=True)
open("spark/columns.txt", "w").write(",".join(cols))

# tables.sql
types = {"flow_id": "BIGINT", "source_day": "STRING", "label": "STRING", "destination_port": "INT"}
body = ",\n".join("  `%s` %s" % (c, types.get(c, "DOUBLE")) for c in cols)

tables = """USE default;

DROP TABLE IF EXISTS flows_batch;
CREATE EXTERNAL TABLE flows_batch (
%s
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/data/raw/flows_batch';

DROP TABLE IF EXISTS flows_logs;
CREATE EXTERNAL TABLE flows_logs (
  `ts` STRING,
%s
) PARTITIONED BY (`date` STRING, `hour` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/data/raw/logs/flows';

MSCK REPAIR TABLE flows_logs;
SHOW PARTITIONS flows_logs;
""" % (body, body)
open("hive/tables.sql", "w").write(tables)

# queries.sql
q = ["USE default;"]
q.append("-- Q1. Row counts (expected: 257265 and 1543587)\n"
         "SELECT 'flows_batch' AS tbl, COUNT(*) AS n FROM flows_batch\n"
         "UNION ALL SELECT 'flows_logs', COUNT(*) FROM flows_logs;")
q.append("-- Q2. Attack distribution, MySQL/Sqoop part (B)\n"
         "SELECT label, COUNT(*) AS n,\n"
         "       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct\n"
         "FROM flows_batch GROUP BY label ORDER BY n DESC;")
q.append("-- Q3. Attack distribution, Flume logs part (A)\n"
         "SELECT label, COUNT(*) AS n,\n"
         "       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct\n"
         "FROM flows_logs GROUP BY label ORDER BY n DESC;")
q.append("-- Q4. Attack distribution, both sources together\n"
         "SELECT label, SUM(n) AS total FROM (\n"
         "  SELECT label, COUNT(*) AS n FROM flows_batch GROUP BY label\n"
         "  UNION ALL\n"
         "  SELECT label, COUNT(*) AS n FROM flows_logs GROUP BY label) u\n"
         "GROUP BY label ORDER BY total DESC;")
q.append("-- Q5. Top 3 destination ports per label (part B)\n"
         "SELECT label, destination_port, n FROM (\n"
         "  SELECT label, destination_port, COUNT(*) AS n,\n"
         "         ROW_NUMBER() OVER (PARTITION BY label ORDER BY COUNT(*) DESC) AS rk\n"
         "  FROM flows_batch GROUP BY label, destination_port) t\n"
         "WHERE rk <= 3 ORDER BY label, n DESC;")
q.append("-- Q6. Top 10 destination ports for attacks only (part B)\n"
         "SELECT destination_port, COUNT(*) AS n FROM flows_batch\n"
         "WHERE UPPER(label) <> 'BENIGN'\n"
         "GROUP BY destination_port ORDER BY n DESC LIMIT 10;")

stats = [c for c in ["flow_duration", "total_fwd_packets", "total_backward_packets",
                     "total_length_of_fwd_packets", "total_length_of_bwd_packets",
                     "flow_bytes_s", "flow_packets_s"] if has(c)]
sel = ",\n       ".join("ROUND(AVG(%s), 1) AS avg_%s" % (c, c) for c in stats)
q.append("-- Q7. Average duration / packets / bytes per label (part B)\n"
         "SELECT label, COUNT(*) AS n,\n       %s\nFROM flows_batch GROUP BY label ORDER BY n DESC;" % sel)

if has("protocol"):
    q.append("-- Q8. Top protocols per label\n"
             "SELECT label, protocol, COUNT(*) AS n FROM flows_batch\n"
             "GROUP BY label, protocol ORDER BY label, n DESC;")
else:
    q.append("-- Q8. Top destination ports overall (the prepared data has no protocol column)\n"
             "SELECT destination_port, COUNT(*) AS n FROM flows_batch\n"
             "GROUP BY destination_port ORDER BY n DESC LIMIT 10;")

q.append("-- Q9. Flow volume per hour as written by Flume\n"
         "SELECT `date`, `hour`, COUNT(*) AS n FROM flows_logs\n"
         "GROUP BY `date`, `hour` ORDER BY `date`, `hour`;")
open("hive/queries.sql", "w").write("\n\n".join(q) + "\n")

print("columns:", len(cols), "| stats used:", stats, "| protocol column:", has("protocol"))







