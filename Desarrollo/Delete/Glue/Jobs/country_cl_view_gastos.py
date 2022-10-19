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
    table_name="mastergeo_countries_customer_geolab_cvargas_country_cl_view_gastos",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Apply Mapping
ApplyMapping_node1649953554504 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("monto", "decimal", "monto", "decimal"),
        ("gse", "string", "gse", "string"),
        ("nombre", "string", "nombre", "string"),
        ("id_gse", "int", "id_gse", "int"),
        ("id_recoba", "int", "id_recoba", "int"),
    ],
    transformation_ctx="ApplyMapping_node1649953554504",
)

# Script generated for node Amazon S3
AmazonS3_node1649953615613 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1649953554504,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://georesearch-datalake/demo_datalake/raw/country_cl/view_gastos/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1649953615613",
)

job.commit()
