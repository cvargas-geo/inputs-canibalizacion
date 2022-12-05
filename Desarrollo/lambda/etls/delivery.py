import os
import boto3
import traceback
# import pandas as pd
import awswrangler as wr
# import sqlalchemy
# from sqlalchemy import create_engine, MetaData
from utils import business_rules as br
from utils.db_utils import db_secret, make_conn  , execute_query, postgres_to_athena
from utils.response import response_error, response_ok
from utils.read import read_templated_file , resolve_stage_db
from utils.athena import athena_to_postres
from utils import conf

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


def etl_delivery(event):

    """Desde la sf se añade el stage y el input queda en "input" """
    stage = event.get('stage', None)
    try:
        input_validation(event)

        schema        = event.get('input', None).get('schema', None)
        report_name   = event.get('input', None).get('report_name', None)
        environment   = event.get('input', None).get('environment', None)
        drop_table    = event.get('input', None).get('drop_workflow', None)
        etl_name    = event.get('input', None).get('etl_name', 'delivery')
        report_name = event.get('input', None).get('report_name', 'delivery')

        db_stage = resolve_stage_db(environment)
        # if is_valid :
        base_dir = os.getcwd()
        sql_queries_dir = f"{base_dir}/sql_queries/athena/"

        etapa1=True
        etapa2=True
        etapa3=True
        # etapa4=True

        if stage == 1 and etapa1 :
            #Nota el generic_path siempre se obtiene a nivel del stage
            generic_path = conf.get_dimanic_sql_path(  etl_name , report_name, stage)

            copy_table = 'zonas_reparto'
            assert postgres_to_athena(copy_table, environment , event["input"] , columns='id_pois ,shape'  ) == True , f"Error al obtener la tabla {copy_table}"

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
            #Nota el generic_path siempre se obtiene a nivel del stage
            generic_path = conf.get_dimanic_sql_path(  etl_name , report_name, stage)
            # if etapa2 :

            if drop_table == False :
                    copy_table = 'zonas_reparto'
                    assert postgres_to_athena(copy_table, environment , event["input"] , columns='id_pois , st_astext(shape) as shape') == True , f"Error al obtener la tabla {copy_table}"
                    copy_table = 'locales_propios'
                    assert postgres_to_athena(copy_table, environment , event["input"] ) == True , f"Error al obtener la tabla {copy_table}"
                # , columns='pois , st_astext(shape) as shape'

            # table_name = 'sp_canibalizacion_reparto'
            table_name = 'gasto_reparto'
            print("Step 1 : Creación de tabla {table_name}")
            print("ETL 1 de 4 : Creación de tabla {table_name}")
            sql_querie = read_templated_file(
                f"{generic_path}02_{etl_name}_{table_name}.sql" ,
                dict({
                    'ETL_NAME': etl_name,
                    'db' : db_stage,
                    'max_gse':br.get_limit_gse_by_country(schema)
                    },
                    **(event.get('input', None)),
                    **(event.get('input', None)['parametros'][etl_name])
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
            "lambda_name":conf.CREATE_ATHENA_TABLE_LAMBDA_NAME
            }

            input_data ={
                "worker_tasks_list": [task_1]
            }
            return input_data

        if stage == 3 and etapa3:
            """
            Fin del proceso de canibalización delivery
            Copia secuencial de las tablas generadas (2 min app) hacia el DW
            """
            if drop_table == False :
                custom_schema_ath = f'{report_name}_{schema}_{etl_name}'
                table_for_copy = 'gasto_reparto'

                print(f"copi from athena {custom_schema_ath}_{table_for_copy} to pg")
                # gym_cl_delivery_sp_canibalizacion_reparto
                df = wr.athena.read_sql_query(f"SELECT * FROM {custom_schema_ath}_{table_for_copy}", database=f"{environment.lower()}_{conf.SERVICE_NAME}")
                # df.fillna('NULL' ,inplace=True)
                custom_schema_pg = f'customer_{report_name}_{schema}'
                athena_to_postres(df , custom_schema_pg , table_for_copy ,credential=db_secret[environment.upper()] )

                # Agrega la columna shape como geometry type si existiera en la tabla de athena
                if "shape_wkt" in df.columns :
                    conn = make_conn(db_secret[environment.upper()])
                    sql_querie = f"""ALTER TABLE {custom_schema_pg}.{table_for_copy}
                                    ADD COLUMN shape geometry;"""
                    execute_query(conn  , sql_querie , {})
                    sql_querie = f"""UPDATE {custom_schema_pg}.{table_for_copy}
                                    SET shape = ST_GeomFromText(shape_wkt);"""
                    execute_query(conn  , sql_querie , {})
                    conn.close()

                response = { "Status": 'Ok, copia athena-pg'}
            else:
                response = { "Status": 'Skip'}

            return response

    except Exception as e:
        return response_error(str(traceback.format_exc()))