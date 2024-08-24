import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724369785200 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724369785200")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724369810517 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724369810517")

# Script generated for node Join
Join_node1724369960065 = Join.apply(frame1=AWSGlueDataCatalog_node1724369810517, frame2=AWSGlueDataCatalog_node1724369785200, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1724369960065")

# Script generated for node Select Fields
SelectFields_node1724370025264 = SelectFields.apply(frame=Join_node1724369960065, paths=["phone", "email"], transformation_ctx="SelectFields_node1724370025264")

# Script generated for node Amazon S3
AmazonS3_node1724370154130 = glueContext.getSink(path="s3://awsglue-stedi/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724370154130")
AmazonS3_node1724370154130.setCatalogInfo(catalogDatabase="database",catalogTableName="customer_curated")
AmazonS3_node1724370154130.setFormat("json")
AmazonS3_node1724370154130.writeFrame(SelectFields_node1724370025264)
job.commit()