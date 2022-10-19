
import boto3
import os 
# import json
# import tracebackpy
# from utils import athena  as  atn 
# from utils import step_functions  as  sf 

# import time
boto3.setup_default_session(profile_name="tecnologia")
# from utils.read import read_templated_file
# from utils.conf import sql_queries_dir  ,s3_etl_output_data , s3_prefix_etl_output_data,S3_BUCKET_DATALAKE, WAITING_TIME_IN_SECONDS
# from utils.glue import glue_client,  delete_a_table_from_database

# s3_client = boto3.resource('s3')
# athena_client = boto3.client('athena')

from test import athena as atn 
from test import glue as g 
from test import consolidar  as c 


# g.test_check_table_exist()
    
# atn.test_create_table()

# atn.test_delete_table()
 
c.consolidar()

