import os
import boto3
import traceback
# import pandas as pd
import awswrangler as wr
import sqlalchemy
# from sqlalchemy import create_engine, MetaData
from utils import business_rules as br
from utils.db_utils import db_secret, make_conn , postgres_to_athena
from utils.read import read_templated_file , resolve_stage_db
from utils.athena import athena_to_postres
from utils.response import response_error, response_ok
from utils import conf
# from utils.conf import (
#     # CREATE_ATHENA_TABLE_LAMBDA_NAME,
#     # S3_BUCKET_DATALAKE ,
#     # conf.get_dimanic_sql_path,
#     # conf.SERVICE_NAME
#     )

lambda_client = boto3.client("lambda")
s3_client = boto3.client('s3')
s3_client_resource = boto3.resource('s3')


def input_validation(event):
    default_params = [
        "stage" ,
        "etl_name" ,
        "input"
    ]
    for param in default_params:
        if param not in event :
            raise ValueError(f"Se espera {param}")

def etl_local(event):

    """Desde la sf se añade el stage y el input queda en "input" """
    stage = event.get('stage', None)
    try:
        input_validation(event)
        schema        = event.get('input', None).get('schema', None)
        report_name   = event.get('input', None).get('report_name', None)
        environment   = event.get('input', None).get('environment', None) 
        drop_table    = event.get('input', None).get('drop_workflow', None) 
        etl_name    = event.get('input', None).get('etl_name', 'local')
        report_name = event.get('input', None).get('report_name', 'local')

        db_stage = resolve_stage_db(environment)

        response = {}

        base_dir = os.getcwd()
        sql_queries_dir = f"{base_dir}/sql_queries/athena/" 
        # id_gastos es obligatorio, en ese caso la key gastos no depende el etl_name
        # id_gastos = parametros['gastos'][schema]['id_gastos']
        etapa1=True
        etapa2=True
        etapa3=True
        etapa4=True

        if stage == 1 and etapa1 :
            #Nota el generic_path siempre se obtiene a nivel del stage
            generic_path = conf.get_dimanic_sql_path(  etl_name , report_name, stage)
            # if etapa1 :
            print("ETL 1 de 4 : Creación de tabla de precálculo")
            print("Step 1 : Creación de tabla de gastos")
            table_name = 'gasto_por_block'

            sql_querie = read_templated_file(
                f"{generic_path}01_{etl_name}_{table_name}.sql" ,
                dict({
                    'ETL_NAME': etl_name,
                    'db' : db_stage,
                    'max_gse':br.get_limit_gse_by_country(schema)
                    },
                    **(event.get('input', None)),
                    **(event.get('input', None)['parametros'][etl_name])
                )
            )

            custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}"
            tabla_anterior = custom_table_name

            print(f"A crear tabla con : {sql_querie}")

            task_1 = {
            "task_name": f" Step 1 : Creación de tabla de gastos {custom_table_name}",
            "worker_parameters": {
                        "report_name": report_name,
                        "table_name": custom_table_name,
                        "sql_query": sql_querie,
                        "drop_table": drop_table,
                        "db_stage": db_stage
                    },
            "lambda_name":conf.CREATE_ATHENA_TABLE_LAMBDA_NAME
            }

            input_data ={
                "worker_tasks_list": [task_1]
            }
            return input_data

        if stage == 2 and etapa2:
            # if etapa2 :
            """Segunda parte, con los gastos por block se hace una intersección con de pois 
            con estos https://xbrein.atlassian.net/wiki/spaces/DAT/pages/1603305473/Canibalizaci+n
            """
            print("Step 2 : Precalculo, locales con gastos ")
            #Nota el generic_path siempre se obtiene a nivel del stage
            generic_path = conf.get_dimanic_sql_path(  etl_name , report_name, stage)
            table_name = 'precalculo'

            tabla_anterior = f"{report_name}_{schema}_{etl_name}_gasto_por_block"
            sql_querie = read_templated_file(
                f"{generic_path}02_{etl_name}_{table_name}.sql" ,
                dict({
                    'ETL_NAME': etl_name,
                    'db' : db_stage,
                    'project_db' : conf.SERVICE_NAME,
                    'tabla_anterior_gastos' : tabla_anterior
                    },
                    **(event.get('input', None)),
                    **(event.get('input', None)['parametros'][etl_name])
                )
            )

            custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}"

            print(f"A crear tabla con : {sql_querie}")

            task_1 = {
            "task_name": f" Step 2 : Precálculo, locales con gastos {custom_table_name}",
            "worker_parameters": {
                        "report_name": report_name,
                        "table_name": custom_table_name,
                        "sql_query": sql_querie,
                        "drop_table": drop_table,
                        "db_stage": db_stage
                    },
            "lambda_name":conf.CREATE_ATHENA_TABLE_LAMBDA_NAME
            }
            input_data = {
                "worker_tasks_list": [task_1]
            }
            return input_data

        if stage == 3 and etapa3:
            # if etapa3 :
            """"En este stage se genera 2 tablas de menor tamaño,
            pero antes es necesario obtener la tabla de locales propios (solo id_pois)"""

            # locales_propiosvf3 para chedraui
            assert postgres_to_athena('locales_propios', environment , event["input"]) , "Error en etapa 3 al copiar de pg to athena"

            #Nota el generic_path siempre se obtiene a nivel del stage
            generic_path = conf.get_dimanic_sql_path(  etl_name , report_name, stage)
            # if etapa3 :
                #TASK 1 , precalculo blocks  ########################
            print("ETL 3 de 3 : Creación de tablas precalculo locales y blocks")

            table_name = 'precalculo_blocks'

            tabla_anterior = f"{report_name}_{schema}_{etl_name}_precalculo"
            sql_querie = read_templated_file(
                f"{generic_path}03_{etl_name}_{table_name}.sql" ,
                dict({
                    'ETL_NAME': etl_name,
                    'db' : db_stage,
                    'project_db' : conf.SERVICE_NAME,
                    'tabla_anterior' : tabla_anterior,
                    },
                    **(event.get('input', None)),
                    **(event.get('input', None)['parametros'][etl_name])
                )
            )
            custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}"

            print(f"A crear tabla con : {sql_querie}")

            task_1 = {
            "task_name": f" Step 3 : Creación de tabla {custom_table_name}",
            "worker_parameters": {
                        "report_name": report_name,
                        "table_name": custom_table_name,
                        "sql_query": sql_querie,
                        "drop_table": drop_table,
                        "db_stage": db_stage
                    },
            "lambda_name":conf.CREATE_ATHENA_TABLE_LAMBDA_NAME
            }
            #TASK 2, precalculo locales ########################
            table_name = 'precalculo_locales'

            tabla_anterior = f"{report_name}_{schema}_{etl_name}_precalculo"
            sql_querie = read_templated_file(
                f"{generic_path}04_{etl_name}_{table_name}.sql" ,
                dict({
                    'ETL_NAME': etl_name,
                    'db' : db_stage,
                    'project_db' : conf.SERVICE_NAME,
                    'tabla_anterior' : tabla_anterior,
                    },
                    **(event.get('input', None))
                )
            )
            custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}"

            print(f"A crear tabla con : {sql_querie}")

            task_2 = {
            "task_name": f" Step 3 : Creación de tabla {custom_table_name}",
            "worker_parameters": {
                        "report_name": report_name,
                        "table_name": custom_table_name,
                        "sql_query": sql_querie,
                        "drop_table": drop_table,
                        "db_stage": db_stage
                    },
            "lambda_name":conf.CREATE_ATHENA_TABLE_LAMBDA_NAME
            }


            input_data ={
                "worker_tasks_list": [task_1,task_2]
            }
            return input_data

        if stage == 4 and etapa4:
            """
            Fin del proceso de canibalización local
            Copia secuencial de las tablas generadas (2 min app) hacia el DW
            """
            for_test_schema = f'customer_{report_name}_{schema}'
            custom_schema = for_test_schema
            """ 1 precalculo blocks"""
            df = wr.athena.read_sql_query(f"SELECT * FROM {report_name}_{schema}_local_precalculo_blocks", database=f"{environment.lower()}_{conf.SERVICE_NAME}")
            # df.fillna('NULL' ,inplace=True)
            athena_to_postres(df , custom_schema , 'precalculo_blocks' ,credential=db_secret[environment.upper()] )
            """ 2 precalculo locales"""
            df = wr.athena.read_sql_query(f"SELECT * FROM {report_name}_{schema}_local_precalculo_locales", database=f"{environment.lower()}_{conf.SERVICE_NAME}")
            # df.fillna('NULL' ,inplace=True)
            athena_to_postres(df , custom_schema , 'precalculo_locales' ,credential=db_secret[environment.upper()] )
            """ 3 gasto_por_block"""
            df = wr.athena.read_sql_query(f"""SELECT
                id,block_id,gasto,shape_wkt --optimizado
                --id,block_id,recoba_id,gasto,longitud,latitud,shape
                FROM {report_name}_{schema}_local_gasto_por_block""", database=f"{environment.lower()}_{conf.SERVICE_NAME}")
            # df.fillna('NULL' ,inplace=True)
            athena_to_postres(df , custom_schema , 'blocks_gasto' ,credential=db_secret[environment.upper()] )

            response = { "Status": 'Ok, copia de datos realizada'}
            return response

    except Exception as e:
        return response_error(str(e))