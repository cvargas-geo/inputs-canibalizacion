import sys
fun_name = lambda n=0: sys._getframe(n + 1).f_code.co_name

# import boto3
# boto3.setup_default_session(profile_name="tecnologia")
# import traceback
# import time
# from utils.read import read_templated_file
# from utils.conf import sql_queries_dir  ,s3_etl_output_data , s3_prefix_etl_output_data,S3_BUCKET_DATALAKE, WAITING_TIME_IN_SECONDS
# from utils.glue import glue_client,  delete_a_table_from_database

from utils import glue  as  g 
# from utils import step_functions  as  sf 


def test_check_table_exist():

    assert g.check_table_exist( 'no_exists','no_exists' )                 == False , f"Error  {fun_name()}, {locals()}"
    assert g.check_table_exist( 'prod_countries','no_exists' )              == False , f"Error  {fun_name()}, {locals()}"
    assert g.check_table_exist( 'no_exists','country_cl_empresas' )       == False , f"Error  {fun_name()}, {locals()}"
    assert g.check_table_exist( 'prod_countries','country_cl_empresas' )    == True  , f"Error  {fun_name()}, {locals()}"


