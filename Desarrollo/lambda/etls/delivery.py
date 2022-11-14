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
                    # print(dict(
                    #         {
                    #         'ETL_NAME': etl_name,
                    #         'db' : db_stage
                    #         },
                    #         **event
                    # ))
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
                    # return {"status":"Ok"}

                    # table_name = 'sp_canibalizacion'
                    # print("Step 1 : Creación de tabla {table_name}")
                    # print("ETL 1 de 4 : Creación de tabla {table_name}")
                    # #Nota el generic_path siempre se obtiene a nivel del stage
                    # generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                    # tabla_anterior = f"{report_name}_{schema}_{etl_name}_local_manzana"
                    # sql_querie = read_templated_file(
                    #     f"{generic_path}02_{etl_name}_{table_name}.sql" ,
                    #     dict({
                    #         'ETL_NAME': etl_name,
                    #         'db' : db_stage,
                    #         'project_db' : SERVICE_NAME,
                    #         'tabla_anterior' : tabla_anterior
                    #         },
                    #         **(event.get('input', None))
                    #     )
                    # )

                    # custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}"

                    # print(f"A crear tabla con : {sql_querie}")

                    # task_1 = {
                    # "task_name": f" Step 2 : Precálculo, locales con gastos {custom_table_name}",
                    # "worker_parameters": {
                    #             "report_name": report_name,
                    #             "table_name": custom_table_name,
                    #             "sql_query": sql_querie,
                    #             "drop_table": drop_table,
                    #             "db_stage": db_stage
                    #         },
                    # "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    # }
                    # input_data = {
                    #     "worker_tasks_list": [task_1]
                    # }
                    # return input_data
            if stage == 4 :
                if etapa4 :
                    """"En este stage se genera 2 tablas de menor tamaño,
                    pero antes es necesario obtener la tabla de locales propios (solo id_pois)"""

                    # if postgres_to_athena1(environment , event):
                    # locales_propiosvf3 para chedraui
                    if postgres_to_athena('locales_propios', environment , event):

                        #Nota el generic_path siempre se obtiene a nivel del stage
                        generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                        if etapa3 :
                            #TASK 1 , precalculo blocks  ########################
                            print("ETL 3 de 3 : Creación de tablas precalculo locales y blocks")

                            table_name = 'precalculo_blocks'
                            # print(dict(
                            #         {
                            #         'ETL_NAME': etl_name,
                            #         'db' : db_stage
                            #         },
                            #         **event
                            # ))
                            tabla_anterior = f"{report_name}_{schema}_{etl_name}_precalculo"
                            sql_querie = read_templated_file(
                                f"{generic_path}03_{etl_name}_{table_name}.sql" ,
                                dict({
                                    'ETL_NAME': etl_name,
                                    'db' : db_stage,
                                    'project_db' : SERVICE_NAME,
                                    'tabla_anterior' : tabla_anterior,
                                    },
                                    **(event.get('input', None))
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
                            "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                            }
                            #TASK 2, precalculo locales ########################
                            table_name = 'precalculo_locales'
                            # print(dict(
                            #         {
                            #         'ETL_NAME': etl_name,
                            #         'db' : db_stage
                            #         },
                            #         **event
                            # ))
                            tabla_anterior = f"{report_name}_{schema}_{etl_name}_precalculo"
                            sql_querie = read_templated_file(
                                f"{generic_path}04_{etl_name}_{table_name}.sql" ,
                                dict({
                                    'ETL_NAME': etl_name,
                                    'db' : db_stage,
                                    'project_db' : SERVICE_NAME,
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
                            "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                            }


                            input_data ={
                                "worker_tasks_list": [task_1,task_2]
                            }
                            return input_data
            if stage == 3 :
                if etapa3 :
                    """
                    Fin del proceso de canibalización delivery
                    Copia secuencial de las tablas generadas (2 min app) hacia el DW
                    """
                    for_test_schema = f'customer_{report_name}_{schema}'
                    custom_schema = for_test_schema
                    """ 1 precalculo blocks"""
                    df = wr.athena.read_sql_query(f"SELECT * FROM {report_name}_{schema}_local_precalculo_blocks", database=f"{environment.lower()}_{SERVICE_NAME}")
                    # df.fillna('NULL' ,inplace=True)
                    athena_to_postres(df , custom_schema , 'precalculo_blocks' ,credential=db_secret[environment.upper()] )
                    """ 2 precalculo locales"""
                    df = wr.athena.read_sql_query(f"SELECT * FROM {report_name}_{schema}_local_precalculo_locales", database=f"{environment.lower()}_{SERVICE_NAME}")
                    # df.fillna('NULL' ,inplace=True)
                    athena_to_postres(df , custom_schema , 'precalculo_locales' ,credential=db_secret[environment.upper()] )
                    """ 3 gasto_por_block"""
                    df = wr.athena.read_sql_query(f"""SELECT
                        id,block_id,gasto,shape --optimizado
                        --id,block_id,recoba_id,gasto,longitud,latitud,shape
                        FROM {report_name}_{schema}_local_gasto_por_block""", database=f"{environment.lower()}_{SERVICE_NAME}")
                    # df.fillna('NULL' ,inplace=True)
                    athena_to_postres(df , custom_schema , 'blocks_gasto' ,credential=db_secret[environment.upper()] )

                    response = { "Status": 'Ok, copia de datos realizada'}
                    return response
        else:
            raise ValueError(f"favor especificar el stage para continuar ")
    except Exception as e:
        e = str(traceback.format_exc())
        print(e)
        return {"Error": e }




# def athena_to_postres1(df , schema , table_name ,credential=None ):
#     try:
#         engine = create_engine(f"postgresql+psycopg2://{credential['username']}:{credential['password']}@{credential['host']}:credential['port']/{credential['dbname']}")
#         meta = sqlalchemy.MetaData(engine, schema=schema)
#         meta.reflect(engine, schema=schema)
#         pdsql = pd.io.sql.SQLDatabase(engine, meta=meta)

#         # df = pd.read_sql("SELECT * FROM xxx", con=engi    ne)
#         # https://stackoverflow.com/questions/71970584/pandas-to-sql-with-dict-raises-cant-adapt-type-dict-is-there-a-way-to-avoi
#         pdsql.to_sql(df, table_name , if_exists='replace' , index=False , dtype={"properties": sqlalchemy.types.JSON})
#     except Exception as e:
#         print("Error al copiar la tabla con sqlalchemy", e)



# def postgres_to_athena1(environment , event):
#     """Ejecuta una consulta a postgres para traer la tabla locales_propios y cargarla en athena"""
#     if environment.upper() in ["PROD","QA"]:
#         conn = make_conn(db_secret[environment.upper()])
#         report_name = event['report_name']
#         schema = event['schema']
#         report_name = event['report_name']
#         sql = f"""SELECT  * FROM customer_{report_name}_{schema}.locales_propios;"""
#         # solo pra el caso de chedraui comentar
#         sql = f"""SELECT  * FROM customer_{report_name}_{schema}.locales_propiosvf3;"""

#         df = pd.read_sql(sql, conn)
#         conn.close()
#         rows , cols = df.shape[0] ,df.shape[1]
#         print(f"Copiando datos locales propios  {rows},{cols}")
#         target_s3_url = f's3://{S3_BUCKET_DATALAKE}/{environment.upper()}/athena_processing/{SERVICE_NAME}/{report_name}_{schema}_locales_propios/'
#         wr.s3.to_parquet(
#             df=df,
#             path=target_s3_url,
#             dataset=True,
#             index=False,
#             sanitize_columns = False,
#             use_threads=True,
#             mode="overwrite",
#             database=f"{environment.lower()}_{SERVICE_NAME}",
#             table=f'{report_name}_{schema}_locales_propios'
#         )
#         ##validación de datos
#         df2 = wr.athena.read_sql_query(
#             f"SELECT * FROM {environment.lower()}_{SERVICE_NAME}.{report_name}_{schema}_locales_propios ",
#             database=f"{environment.lower()}_{SERVICE_NAME}"
#         )
#         if rows != df2.shape[0] or cols != df2.shape[1] :
#             raise Exception(f"Ocurrió un problema al copiar los datos de locales propios , {df2.shape}")
#     else:
#         raise Exception("Bad string environment given")

#     return True