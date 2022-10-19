import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv, [ 'JOB_NAME',
        'SOURCE_DB',
        'SOURCE_TABLE',

        'SOURCE_BUCKET',

        'TARGET_TABLE',
        'TARGET_GLUE_DB'
    ])
    

SOURCE_DB=  args["SOURCE_DB"]
SOURCE_TABLE =   args["SOURCE_TABLE"]
SOURCE_BUCKET=  args["SOURCE_BUCKET"]
TARGET_TABLE=  args["TARGET_TABLE"]
TARGET_GLUE_DB=  args["TARGET_GLUE_DB"]

print(args)    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DB,
    table_name=SOURCE_TABLE,
    transformation_ctx="DataCatalogtable_node1",
)
##Borrar datos previos 
s3_path1 = SOURCE_BUCKET 
print("Tablas borradas")
glueContext.purge_s3_path(s3_path1, {"retentionPeriod": -1})


# Script generated for node Amazon S3
AmazonS3_node1649953615613 = glueContext.getSink(
    path=SOURCE_BUCKET,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1649953615613",
)
AmazonS3_node1649953615613.setCatalogInfo(
    catalogDatabase=TARGET_GLUE_DB, catalogTableName=TARGET_TABLE
)
AmazonS3_node1649953615613.setFormat("glueparquet")
AmazonS3_node1649953615613.writeFrame(DataCatalogtable_node1)
job.commit()
