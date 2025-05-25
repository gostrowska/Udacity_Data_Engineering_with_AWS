import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1748170671025 = glueContext.create_dynamic_frame.from_catalog(database="graza", table_name="step_trainer_trusted", transformation_ctx="AmazonS3_node1748170671025")

# Script generated for node Amazon S3
AmazonS3_node1748170673394 = glueContext.create_dynamic_frame.from_catalog(database="graza", table_name="accelerometer_trusted", transformation_ctx="AmazonS3_node1748170673394")

# Script generated for node Join tables
SqlQuery472 = '''
select * from step_trainer_trusted 
join accelerometer_trusted 
on step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp

'''
Jointables_node1748170437914 = sparkSqlQuery(glueContext, query = SqlQuery472, mapping = {"step_trainer_trusted":AmazonS3_node1748170671025, "accelerometer_trusted":AmazonS3_node1748170673394}, transformation_ctx = "Jointables_node1748170437914")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Jointables_node1748170437914, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1748170129425", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1748170453009 = glueContext.getSink(path="s3://graza-bucket/ML/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1748170453009")
AmazonS3_node1748170453009.setCatalogInfo(catalogDatabase="graza",catalogTableName="ML_curated")
AmazonS3_node1748170453009.setFormat("json")
AmazonS3_node1748170453009.writeFrame(Jointables_node1748170437914)
job.commit()