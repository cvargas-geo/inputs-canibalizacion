import os
import boto3
import traceback
import pandas as pd
import awswrangler as wr
import sqlalchemy
from sqlalchemy import create_engine, MetaData
from utils import business_rules as br
from utils.db_utils import db_secret, make_conn , postgres_to_athena
from utils.read import read_templated_file , resolve_stage_db
from utils.athena import athena_to_postres
from utils.conf import (
    CREATE_ATHENA_TABLE_LAMBDA_NAME,
    S3_BUCKET_DATALAKE ,
    get_dimanic_sql_path,
    SERVICE_NAME
    )

lambda_client = boto3.client("lambda")
s3_client = boto3.client('s3')
s3_client_resource = boto3.resource('s3')

def etl_delivery(event):

    """Desde la sf se añade el stage y el input queda en "input" """
    stage = event.get('stage', None)

    schema        = event.get('input', None).get('schema', None)
    report_name   = event.get('input', None).get('report_name', None)
    environment   = event.get('input', None).get('environment', None)
    # buffer_search = event.get('input', None).get('buffer_search', None)
    drop_table    = event.get('input', None).get('drop_workflow', None)
    # parametros = event #Reemplaza a id_gastos
    # id_gastos = event.get('id_gastos', None) #TODO VALIDAR ESTE CAMPO
    # parametros = event.get('parametros', None) #Reemplaza a id_gastos
    etl_name    = event.get('input', None).get('etl_name', 'delivery')
    report_name = event.get('input', None).get('report_name', 'delivery')

    db_stage = resolve_stage_db(environment)

    response = {}
    result = {}

    try:
        base_dir = os.getcwd()
        sql_queries_dir = f"{base_dir}/sql_queries/athena/" 
        # id_gastos es obligatorio, en ese caso la key gastos no depende el etl_name
        # id_gastos = parametros['gastos'][schema]['id_gastos']
        etapa1=True
        etapa2=True
        etapa3=True
        etapa4=True

        if stage:
            if stage == 1 :
                #Nota el generic_path siempre se obtiene a nivel del stage
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                if etapa1 :

                    table_name = 'gasto_por_block' 
                    sql_querie = read_templated_file(
                        f"{generic_path}01_{etl_name}_{table_name}.sql" ,
                        dict({
                            'ETL_NAME': etl_name,
                            'db' : db_stage,
                            'max_gse':br.get_limit_gse_by_country(schema)
                            },
                            **(event.get('input', None))
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
                    "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    }

                    input_data ={
                        "worker_tasks_list": [task_1]
                    }
                    return input_data

            if stage == 2 :
                #Nota el generic_path siempre se obtiene a nivel del stage
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                if etapa2 :

                    if drop_table == False :
                        assert postgres_to_athena(f'reparto_idpois', environment , event , columns='pois , st_astext(shape) as shape') == True , "Error al obtener la tabla reparto_idpois"
                        assert postgres_to_athena(f'locales_idpois', environment , event ) == True , "Error al obtener la tabla locales_idpois"
                    # , columns='pois , st_astext(shape) as shape'

                    table_name = 'sp_canibalizacion_reparto'
                    print("Step 1 : Creación de tabla {table_name}")
                    print("ETL 1 de 4 : Creación de tabla {table_name}")
                    sql_querie = read_templated_file(
                        f"{generic_path}02_{etl_name}_{table_name}.sql" ,
                        dict({
                            'ETL_NAME': etl_name,
                            'db' : db_stage,
                            'max_gse':br.get_limit_gse_by_country(schema)
                            },
                            **(event.get('input', None))
                        )
                    ) if drop_table == False else  '--No hay tabla que consultar'

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
                    "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    }

                    input_data ={
                        "worker_tasks_list": [task_1]
                    }
                    return input_data

            if stage == 3 :
                if etapa3 :
                    """
                    Fin del proceso de canibalización delivery
                    Copia secuencial de las tablas generadas (2 min app) hacia el DW
                    """
                    if drop_table == False :
                        custom_schema = f'customer_{report_name}_{schema}'
                        """ 1 sp_canibalizacion_reparto"""
                        # gym_cl_delivery_sp_canibalizacion_reparto
                        df = wr.athena.read_sql_query(f"SELECT * FROM {report_name}_{schema}_{etl_name}_sp_canibalizacion_reparto", database=f"{environment.lower()}_{SERVICE_NAME}")
                        # df.fillna('NULL' ,inplace=True)
                        athena_to_postres(df , custom_schema , 'sp_canibalizacion_reparto' ,credential=db_secret[environment.upper()] )
                        response = { "Status": 'Ok, copia athena-pg'}
                    else:
                        response = { "Status": 'Skip'}

                    return response

        else:
            raise ValueError(f"favor especificar el stage para continuar ")
    except Exception as e:
        e = str(traceback.format_exc())
        print(e)
        return {"Error": e }