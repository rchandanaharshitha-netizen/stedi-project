CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing_landing (
  serialnumber              STRING,
  sharewithpublicasofdate   BIGINT,
  birthday                  STRING,
  registrationdate          BIGINT,
  sharewithresearchasofdate BIGINT,
  customername              STRING,
  email                     STRING,
  lastupdatedate            BIGINT,
  phone                     STRING,
  sharewithfriendsasofdate  BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-project-chandana/customer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');