import csv
import os

src = os.path.join("data", "prepared", "part_mysql.csv")
with open(src, newline="") as f:
    header = next(csv.reader(f))

types = {"flow_id": "BIGINT NOT NULL", "source_day": "VARCHAR(60)",
         "label": "VARCHAR(40)", "destination_port": "INT"}
cols = ["  `%s` %s" % (h, types.get(h, "DOUBLE")) for h in header]
cols.append("  PRIMARY KEY (`flow_id`)")
ddl = ("DROP TABLE IF EXISTS flows_batch;\nCREATE TABLE flows_batch (\n"
       + ",\n".join(cols) + "\n);\n")

names = ", ".join("@label" if h == "label" else "`%s`" % h for h in header)
load = ("LOAD DATA INFILE '/var/lib/mysql-files/part_mysql.csv' INTO TABLE flows_batch "
        "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 LINES (%s) "
        "SET `label` = TRIM(TRAILING '\\r' FROM @label);\n" % names)

with open(os.path.join("data", "prepared", "flows_batch.sql"), "w") as out:
    out.write(ddl + load)
print("OK : %d colonnes" % len(header))
