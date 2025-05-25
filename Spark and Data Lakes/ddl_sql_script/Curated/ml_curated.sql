CREATE EXTERNAL TABLE `ml_curated`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://graza-bucket/ML/curated/'
TBLPROPERTIES (
  'CreatedByJob'='Machine Learning Curated Zone', 
  'CreatedByJobRun'='jr_26c5a863386607e74ad71e46b9cb3dbda1b96f516bfecd40ca1d8063f118bc94', 
  'classification'='json')