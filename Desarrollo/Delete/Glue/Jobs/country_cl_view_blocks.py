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
    table_name="mastergeo_countries_customer_geolab_cvargas_country_cl_view_blocks",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node Apply Mapping
ApplyMapping_node1649953554504 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("total_households", "double", "total_households", "double"),
        ("hog_gse6", "double", "hog_gse6", "double"),
        ("date", "timestamp", "date", "timestamp"),
        ("hog_gse7", "double", "hog_gse7", "double"),
        ("pob_edad41", "double", "pob_edad41", "double"),
        ("pob_edad40", "double", "pob_edad40", "double"),
        ("hog_gse2", "double", "hog_gse2", "double"),
        ("pob_edad43", "double", "pob_edad43", "double"),
        ("hog_gse3", "double", "hog_gse3", "double"),
        ("pob_edad42", "double", "pob_edad42", "double"),
        ("hog_gse4", "double", "hog_gse4", "double"),
        ("pob_edad45", "double", "pob_edad45", "double"),
        ("hog_gse5", "double", "hog_gse5", "double"),
        ("pob_edad44", "double", "pob_edad44", "double"),
        ("total_women", "double", "total_women", "double"),
        ("hog_gse1", "double", "hog_gse1", "double"),
        ("longitud", "decimal", "longitud", "decimal"),
        ("pob_edad36", "double", "pob_edad36", "double"),
        ("pob_edad35", "double", "pob_edad35", "double"),
        ("pob_edad38", "double", "pob_edad38", "double"),
        ("id", "int", "id", "int"),
        ("pob_edad37", "double", "pob_edad37", "double"),
        ("pob_edad39", "double", "pob_edad39", "double"),
        ("area", "double", "area", "double"),
        ("pob_edad30", "double", "pob_edad30", "double"),
        ("latitud", "decimal", "latitud", "decimal"),
        ("pob_gse6", "double", "pob_gse6", "double"),
        ("pob_edad32", "double", "pob_edad32", "double"),
        ("pob_gse5", "double", "pob_gse5", "double"),
        ("pob_edad31", "double", "pob_edad31", "double"),
        ("pob_edad34", "double", "pob_edad34", "double"),
        ("pob_gse7", "double", "pob_gse7", "double"),
        ("pob_edad33", "double", "pob_edad33", "double"),
        ("pob_gse2", "double", "pob_gse2", "double"),
        ("pob_gse1", "double", "pob_gse1", "double"),
        ("pob_gse4", "double", "pob_gse4", "double"),
        ("pob_gse3", "double", "pob_gse3", "double"),
        ("var_cen", "double", "var_cen", "double"),
        ("perimeter", "double", "perimeter", "double"),
        ("pob_edad25", "double", "pob_edad25", "double"),
        ("pob_edad24", "double", "pob_edad24", "double"),
        ("pob_edad27", "double", "pob_edad27", "double"),
        ("total_population", "double", "total_population", "double"),
        ("pob_edad26", "double", "pob_edad26", "double"),
        ("pob_edad29", "double", "pob_edad29", "double"),
        ("pob_edad28", "double", "pob_edad28", "double"),
        ("pxq_density_average", "double", "pxq_density_average", "double"),
        ("pob_edad21", "double", "pob_edad21", "double"),
        ("pob_edad20", "double", "pob_edad20", "double"),
        ("pob_edad23", "double", "pob_edad23", "double"),
        ("total_men", "double", "total_men", "double"),
        ("pob_edad22", "double", "pob_edad22", "double"),
        ("income_level", "string", "income_level", "string"),
        ("recoba_id", "int", "recoba_id", "int"),
        ("shape_wkt", "string", "shape_wkt", "string"),
        ("centroid_wkt", "string", "centroid_wkt", "string"),
        ("pob_edad14", "double", "pob_edad14", "double"),
        ("pob_edad13", "double", "pob_edad13", "double"),
        ("pob_edad16", "double", "pob_edad16", "double"),
        ("pob_edad15", "double", "pob_edad15", "double"),
        ("pxq_average", "double", "pxq_average", "double"),
        ("pob_edad18", "double", "pob_edad18", "double"),
        ("pob_edad17", "double", "pob_edad17", "double"),
        ("pxq_density", "double", "pxq_density", "double"),
        ("pob_edad19", "double", "pob_edad19", "double"),
        ("pob_edad1", "double", "pob_edad1", "double"),
        ("density", "double", "density", "double"),
        ("pob_edad2", "double", "pob_edad2", "double"),
        ("pob_edad10", "double", "pob_edad10", "double"),
        ("pob_edad3", "double", "pob_edad3", "double"),
        ("pob_edad4", "double", "pob_edad4", "double"),
        ("pob_edad12", "double", "pob_edad12", "double"),
        ("pob_edad5", "double", "pob_edad5", "double"),
        ("pob_edad11", "double", "pob_edad11", "double"),
        ("pob_edad6", "double", "pob_edad6", "double"),
        ("pxq", "double", "pxq", "double"),
        ("pob_edad7", "double", "pob_edad7", "double"),
        ("pob_edad8", "double", "pob_edad8", "double"),
        ("pob_edad9", "double", "pob_edad9", "double"),
        ("block_id", "int", "block_id", "int"),
        (
            "administrative_area_level_2",
            "string",
            "administrative_area_level_2",
            "string",
        ),
        (
            "administrative_area_level_3",
            "string",
            "administrative_area_level_3",
            "string",
        ),
        (
            "administrative_area_level_1",
            "string",
            "administrative_area_level_1",
            "string",
        ),
        ("pob_edad47", "double", "pob_edad47", "double"),
        ("pob_edad46", "double", "pob_edad46", "double"),
        (
            "administrative_area_level_4",
            "string",
            "administrative_area_level_4",
            "string",
        ),
        ("pob_edad49", "double", "pob_edad49", "double"),
        (
            "administrative_area_level_5",
            "string",
            "administrative_area_level_5",
            "string",
        ),
        ("pob_edad48", "double", "pob_edad48", "double"),
    ],
    transformation_ctx="ApplyMapping_node1649953554504",
)

# Script generated for node Amazon S3
AmazonS3_node1649953615613 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1649953554504,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://georesearch-datalake/demo_datalake/raw/country_cl/view_blocks/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1649953615613",
)

job.commit()
