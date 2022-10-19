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
    table_name="mastergeo_countries_customer_geolab_cvargas_country_cl_pois_comercios_servicios_view",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Apply Mapping
ApplyMapping_node1649953554504 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("ubicacion", "string", "ubicacion", "string"),
        ("estado", "string", "estado", "string"),
        ("tipo", "int", "tipo", "int"),
        ("pois_state_id", "int", "pois_state_id", "int"),
        ("sales_area", "double", "sales_area", "double"),
        ("formato", "string", "formato", "string"),
        ("icon", "string", "icon", "string"),
        ("imputado", "int", "imputado", "int"),
        ("provincia", "string", "provincia", "string"),
        ("nombre", "string", "nombre", "string"),
        ("shape_wkt", "string", "shape_wkt", "string"),
        ("nombre_cadena", "string", "nombre_cadena", "string"),
        ("clase", "string", "clase", "string"),
        ("longitud", "decimal", "longitud", "decimal"),
        ("centroid_wkt", "string", "centroid_wkt", "string"),
        ("category_id", "int", "category_id", "int"),
        ("wsp", "string", "wsp", "string"),
        ("web", "string", "web", "string"),
        ("otro_1", "string", "otro_1", "string"),
        ("otro_2", "string", "otro_2", "string"),
        ("id", "int", "id", "int"),
        ("ig", "string", "ig", "string"),
        ("telefono_2", "string", "telefono_2", "string"),
        ("telefono_1", "string", "telefono_1", "string"),
        ("piso", "string", "piso", "string"),
        ("substring_id", "int", "substring_id", "int"),
        ("mail_1", "string", "mail_1", "string"),
        ("latitud", "decimal", "latitud", "decimal"),
        ("mail_2", "string", "mail_2", "string"),
        ("categoria", "string", "categoria", "string"),
        ("direccion", "string", "direccion", "string"),
        ("address_id", "int", "address_id", "int"),
        ("anexo_direccion", "string", "anexo_direccion", "string"),
        ("subcadena", "string", "subcadena", "string"),
        ("url", "string", "url", "string"),
        ("celular_2", "string", "celular_2", "string"),
        ("fecha", "string", "fecha", "string"),
        ("celular_1", "string", "celular_1", "string"),
        ("box_area", "double", "box_area", "double"),
        ("comuna", "string", "comuna", "string"),
        ("pois_padre", "int", "pois_padre", "int"),
        ("region", "string", "region", "string"),
        ("fb", "string", "fb", "string"),
    ],
    transformation_ctx="ApplyMapping_node1649953554504",
)

# Script generated for node Amazon S3
AmazonS3_node1649953615613 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1649953554504,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://georesearch-datalake/demo_datalake/raw/country_cl/pois_comercios_servicios_view/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1649953615613",
)

job.commit()
