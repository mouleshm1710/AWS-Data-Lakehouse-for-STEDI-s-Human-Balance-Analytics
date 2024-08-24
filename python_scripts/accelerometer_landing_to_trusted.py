import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724286864616 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1724286864616")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724286787244 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724286787244")

# Script generated for node Join
Join_node1724286899969 = Join.apply(frame1=AWSGlueDataCatalog_node1724286864616, frame2=AWSGlueDataCatalog_node1724286787244, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1724286899969")

# Script generated for node Select Fields
SelectFields_node1724287089196 = SelectFields.apply(frame=Join_node1724286899969, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="SelectFields_node1724287089196")

# Script generated for node Drop Duplicates
DropDuplicates_node1724287128283 =  DynamicFrame.fromDF(SelectFields_node1724287089196.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1724287128283")

# Script generated for node Amazon S3
AmazonS3_node1724287187365 = glueContext.getSink(path="s3://awsglue-stedi/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724287187365")
AmazonS3_node1724287187365.setCatalogInfo(catalogDatabase="database",catalogTableName="accelerometer_trusted")
AmazonS3_node1724287187365.setFormat("json")
AmazonS3_node1724287187365.writeFrame(DropDuplicates_node1724287128283)
job.commit()