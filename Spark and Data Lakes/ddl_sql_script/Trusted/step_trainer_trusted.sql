CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://graza-bucket/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step_trainer to Trusted Zone', 
  'CreatedByJobRun'='jr_cc412143d66d6d6a3a56c0a43eeebc0beb083211dbadf915604eba13d4657639', 
  'classification'='json')