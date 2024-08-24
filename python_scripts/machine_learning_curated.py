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

# Script generated for node accelero
accelero_node1724379376558 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="accelerometer_trusted", transformation_ctx="accelero_node1724379376558")

# Script generated for node step
step_node1724379406330 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="step_trainer_trusted", transformation_ctx="step_node1724379406330")

# Script generated for node SQL Query
SqlQuery145 = '''
select distinct s.*,a.* from step s inner join acc a on 
s.sensorreadingtime = a.timestamp;
'''
SQLQuery_node1724379429609 = sparkSqlQuery(glueContext, query = SqlQuery145, mapping = {"step":step_node1724379406330, "acc":accelero_node1724379376558}, transformation_ctx = "SQLQuery_node1724379429609")

# Script generated for node Amazon S3
AmazonS3_node1724379765711 = glueContext.getSink(path="s3://awsglue-stedi/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724379765711")
AmazonS3_node1724379765711.setCatalogInfo(catalogDatabase="database",catalogTableName="machine_learning_curated")
AmazonS3_node1724379765711.setFormat("json")
AmazonS3_node1724379765711.writeFrame(SQLQuery_node1724379429609)
job.commit()