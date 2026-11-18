CREATE TABLE IF NOT EXISTS alerts (
  flow_id BIGINT NOT NULL,
  event_ts VARCHAR(32) NOT NULL,
  source_day VARCHAR(64) NOT NULL,
  predicted_label VARCHAR(64) NOT NULL,
  actual_label VARCHAR(64),
  confidence DOUBLE,
  PRIMARY KEY (flow_id)
);
