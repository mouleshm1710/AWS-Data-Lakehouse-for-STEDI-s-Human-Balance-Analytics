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
AWSGlueDataCatalog_node1724378284377 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1724378284377")

# Script generated for node cust_curated
cust_curated_node1724378401923 = glueContext.create_dynamic_frame.from_catalog(database="database", table_name="customer_curated", transformation_ctx="cust_curated_node1724378401923")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1724378429211 = ApplyMapping.apply(frame=cust_curated_node1724378401923, mappings=[("customername", "string", "right_customername", "string"), ("email", "string", "right_email", "string"), ("phone", "string", "right_phone", "string"), ("birthday", "string", "right_birthday", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("registrationdate", "long", "right_registrationdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforJoin_node1724378429211")

# Script generated for node SQL Query
SqlQuery274 = '''
select distinct s.* from cust c inner join step s 
on c.right_serialnumber = s.serialnumber;


'''
SQLQuery_node1724378923180 = sparkSqlQuery(glueContext, query = SqlQuery274, mapping = {"cust":RenamedkeysforJoin_node1724378429211, "step":AWSGlueDataCatalog_node1724378284377}, transformation_ctx = "SQLQuery_node1724378923180")

# Script generated for node Amazon S3
AmazonS3_node1724375212610 = glueContext.getSink(path="s3://awsglue-stedi/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724375212610")
AmazonS3_node1724375212610.setCatalogInfo(catalogDatabase="database",catalogTableName="step_trainer_trusted")
AmazonS3_node1724375212610.setFormat("json")
AmazonS3_node1724375212610.writeFrame(SQLQuery_node1724378923180)
job.commit()