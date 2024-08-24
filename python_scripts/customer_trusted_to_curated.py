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

# Script generated for node customers
customers_node1724372294839 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="customer_trusted", transformation_ctx="customers_node1724372294839")

# Script generated for node accelero
accelero_node1724372267981 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="accelerometer_trusted", transformation_ctx="accelero_node1724372267981")

# Script generated for node SQL Query
SqlQuery441 = '''
select distinct c.* from cust c inner join acc a 
on c.email = a.user;
'''
SQLQuery_node1724372340245 = sparkSqlQuery(glueContext, query = SqlQuery441, mapping = {"cust":customers_node1724372294839, "acc":accelero_node1724372267981}, transformation_ctx = "SQLQuery_node1724372340245")

# Script generated for node Amazon S3
AmazonS3_node1724372656432 = glueContext.getSink(path="s3://awsglue-stedi/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724372656432")
AmazonS3_node1724372656432.setCatalogInfo(catalogDatabase="database",catalogTableName="customer_curated")
AmazonS3_node1724372656432.setFormat("json")
AmazonS3_node1724372656432.writeFrame(SQLQuery_node1724372340245)
job.commit()