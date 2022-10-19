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
## @type: DataSource
## @args: [database = qa_mapeo_migracion_postgres, table_name = mastergeo_countries_customer_geolab_cvargas_country_cl_categories, transformation_ctx = DataSource0]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(
    database = qa_mapeo_migracion_postgres, 
    table_name = mastergeo_countries_customer_geolab_cvargas_country_cl_categories, 
    transformation_ctx = DataSource0)
job.commit()