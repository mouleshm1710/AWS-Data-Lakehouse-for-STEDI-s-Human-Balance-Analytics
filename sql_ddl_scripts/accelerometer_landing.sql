CREATE EXTERNAL TABLE accelerometer_landing (
    user STRING,
    timestamp BIGINT,
    x FLOAT,
    y FLOAT,
    z FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
) LOCATION 's3://awsglue-stedi/accelerometer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');