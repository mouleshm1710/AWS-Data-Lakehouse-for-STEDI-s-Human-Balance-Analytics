import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724283984835 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1724283984835")

# Script generated for node SQL Query
SqlQuery242 = '''
select * from customer_landing where 
customer_landing.shareWithResearchAsOfDate is not null;
'''
SQLQuery_node1724285187276 = sparkSqlQuery(glueContext, query = SqlQuery242, mapping = {"customer_landing":AWSGlueDataCatalog_node1724283984835}, transformation_ctx = "SQLQuery_node1724285187276")

# Script generated for node Amazon S3
AmazonS3_node1724286059760 = glueContext.getSink(path="s3://awsglue-stedi/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724286059760")
AmazonS3_node1724286059760.setCatalogInfo(catalogDatabase="database",catalogTableName="customer_trusted")
AmazonS3_node1724286059760.setFormat("json")
AmazonS3_node1724286059760.writeFrame(SQLQuery_node1724285187276)
job.commit()