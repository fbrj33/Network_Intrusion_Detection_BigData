USE default;

-- Q1. Row counts (expected: 257265 and 1543587)
SELECT 'flows_batch' AS tbl, COUNT(*) AS n FROM flows_batch
UNION ALL SELECT 'flows_logs', COUNT(*) FROM flows_logs;

-- Q2. Attack distribution, MySQL/Sqoop part (B)
SELECT label, COUNT(*) AS n,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM flows_batch GROUP BY label ORDER BY n DESC;

-- Q3. Attack distribution, Flume logs part (A)
SELECT label, COUNT(*) AS n,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM flows_logs GROUP BY label ORDER BY n DESC;

-- Q4. Attack distribution, both sources together
SELECT label, SUM(n) AS total FROM (
  SELECT label, COUNT(*) AS n FROM flows_batch GROUP BY label
  UNION ALL
  SELECT label, COUNT(*) AS n FROM flows_logs GROUP BY label) u
GROUP BY label ORDER BY total DESC;

-- Q5. Top 3 destination ports per label (part B)
SELECT label, destination_port, n FROM (
  SELECT label, destination_port, COUNT(*) AS n,
         ROW_NUMBER() OVER (PARTITION BY label ORDER BY COUNT(*) DESC) AS rk
  FROM flows_batch GROUP BY label, destination_port) t
WHERE rk <= 3 ORDER BY label, n DESC;

-- Q6. Top 10 destination ports for attacks only (part B)
SELECT destination_port, COUNT(*) AS n FROM flows_batch
WHERE UPPER(label) <> 'BENIGN'
GROUP BY destination_port ORDER BY n DESC LIMIT 10;

-- Q7. Average duration / packets / bytes per label (part B)
SELECT label, COUNT(*) AS n,
       ROUND(AVG(flow_duration), 1) AS avg_flow_duration,
       ROUND(AVG(total_fwd_packets), 1) AS avg_total_fwd_packets,
       ROUND(AVG(total_backward_packets), 1) AS avg_total_backward_packets,
       ROUND(AVG(total_length_of_fwd_packets), 1) AS avg_total_length_of_fwd_packets,
       ROUND(AVG(total_length_of_bwd_packets), 1) AS avg_total_length_of_bwd_packets,
       ROUND(AVG(flow_bytes_s), 1) AS avg_flow_bytes_s,
       ROUND(AVG(flow_packets_s), 1) AS avg_flow_packets_s
FROM flows_batch GROUP BY label ORDER BY n DESC;

-- Q8. Top destination ports overall (the prepared data has no protocol column)
SELECT destination_port, COUNT(*) AS n FROM flows_batch
GROUP BY destination_port ORDER BY n DESC LIMIT 10;

-- Q9. Flow volume per hour as written by Flume
SELECT `date`, `hour`, COUNT(*) AS n FROM flows_logs
GROUP BY `date`, `hour` ORDER BY `date`, `hour`;
