CREATE EXTERNAL TABLE `customer_curated`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://graza-bucket/customer/curated/'
TBLPROPERTIES (
  'CreatedByJob'='Customer Curated Zone', 
  'CreatedByJobRun'='jr_ef37ca53e15c4075a288744572551742ff50d25112ac849861d0add616af2ee3', 
  'classification'='json')