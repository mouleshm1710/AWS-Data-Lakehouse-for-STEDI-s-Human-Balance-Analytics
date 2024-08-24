CREATE EXTERNAL TABLE step_trainer_landing (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
) LOCATION 's3://awsglue-stedi/step_trainer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');