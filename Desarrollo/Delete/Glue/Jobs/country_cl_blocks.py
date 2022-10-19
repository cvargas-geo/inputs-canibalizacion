import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="qa_mapeo_migracion_postgres",
    table_name="mastergeo_countries_customer_geolab_cvargas_country_cl_blocks",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1649953615613 = glueContext.getSink(
    path="s3://georesearch-datalake/demo_datalake/raw/mastergeo_countries_customer_geolab_cvargas_country_cl_blocks/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1649953615613",
)
AmazonS3_node1649953615613.setCatalogInfo(
    catalogDatabase="qa_migracion_postgres", catalogTableName="country_cl_blocks"
)
AmazonS3_node1649953615613.setFormat("glueparquet")
AmazonS3_node1649953615613.writeFrame(DataCatalogtable_node1)
job.commit()
