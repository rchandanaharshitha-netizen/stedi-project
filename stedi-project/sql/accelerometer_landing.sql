CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_trusted (
  timestamp  BIGINT,
  user       STRING,
  x          DOUBLE,
  y          DOUBLE,
  z          DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-chandana/accelerometer/trusted/'
TBLPROPERTIES ('has_encrypted_data'='false');