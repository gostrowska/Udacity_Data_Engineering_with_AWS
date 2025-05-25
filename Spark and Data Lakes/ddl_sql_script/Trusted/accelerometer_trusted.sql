CREATE EXTERNAL TABLE `accelerometer_trusted`(
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
  's3://graza-bucket/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Trusted Zone', 
  'CreatedByJobRun'='jr_c3347a64948d184ba8c552806fa0e9bafa0e99ad8f13e5fcbec4106e33f5acc1', 
  'classification'='json')