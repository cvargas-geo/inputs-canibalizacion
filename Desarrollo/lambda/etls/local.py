import os
import boto3 
import traceback 
import awswrangler as wr
from utils.read import read_templated_file , resolve_stage_db
from utils.custom import list_to_sql_in , consolidar_trim_commas
from utils.conf import (
    TARGET_DB,DATALAKE_DB ,
    CREATE_ATHENA_TABLE_LAMBDA_NAME,
    s3_etl_output_data,
    S3_BUCKET_DATALAKE ,
    s3_prefix_delivery_output_data,
    s3_prefix_etl_output_data,
    get_dimanic_sql_path
    )

lambda_client = boto3.client("lambda")

s3_client = boto3.client('s3')
s3_client_resource = boto3.resource('s3')

def etl_local(event):

    """Desde la sf se añade el stage y el input queda en "input" """
    stage = event.get('stage', None)
    db_stage = resolve_stage_db(event.get('input', None).get('environment', None))

    schema        = event.get('input', None).get('schema', None)
    report_name   = event.get('input', None).get('report_name', None)
    buffer_search = event.get('input', None).get('buffer_search', None)
    drop_table    = event.get('input', None).get('drop_workflow', None)
    # parametros = event #Reemplaza a id_gastos
    # id_gastos = event.get('id_gastos', None) #TODO VALIDAR ESTE CAMPO
    # parametros = event.get('parametros', None) #Reemplaza a id_gastos
    etl_name    = event.get('input', None).get('etl_name', 'local')
    report_name = event.get('input', None).get('report_name', 'local')
    response = {}
    result = {}

    try:
        base_dir = os.getcwd()
        sql_queries_dir = f"{base_dir}/sql_queries/athena/" 
        # id_gastos es obligatorio, en ese caso la key gastos no depende el etl_name
        # id_gastos = parametros['gastos'][schema]['id_gastos']
        etapa1=True
        etapa2=True

        if stage:
            if stage == 1 :
                #Nota el generic_path siempre se obtiene a nivel del stage
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                if etapa1 :
                    print("ETL 1 de 4 : Creación de tabla de precálculo")
                    print("Step 1 : Creación de tabla de gastos")
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
                            'db' : db_stage},
                            **(event.get('input', None))
                        )
                    )

                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}"

                    print(f"A crear tabla con : {sql_querie}")

                    task_1 = {
                    "task_name": f" Step 1 : Creación de tabla de gastos {custom_table_name}",
                    "worker_parameters": {
                                "report_name": report_name,
                                "table_name": custom_table_name,
                                "sql_query": sql_querie,
                                "drop_table": drop_table
                            },
                    "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    }

                    input_data ={
                        "worker_tasks_list": [task_1]
                    }
                    return input_data


            if stage == 2 :
                if etapa2 :
                    """Segunda parte, con los gastos por block se hace una intersección con de pois 
                    con estos https://xbrein.atlassian.net/wiki/spaces/DAT/pages/1603305473/Canibalizaci+n
                    """
                    print("Step 2 : Precalculo, locales con gastos ")
                    table_name = 'precalculo'
                    # print(dict(
                    #         {
                    #         'ETL_NAME': etl_name,
                    #         'db' : db_stage
                    #         },
                    #         **event
                    # ))
                    sql_querie = read_templated_file(
                        f"{generic_path}02_{etl_name}_{table_name}.sql" ,
                        dict({
                            'ETL_NAME': etl_name,
                            'db' : db_stage},
                            **(event.get('input', None))
                        )
                    )

                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}"

                    print(f"A crear tabla con : {sql_querie}")

                    task_1 = {
                    "task_name": f" Step 1 : Creación de tabla de gastos {custom_table_name}",
                    "worker_parameters": {
                                "report_name": report_name,
                                "table_name": custom_table_name,
                                "sql_query": sql_querie,
                                "drop_table": drop_table
                            },
                    "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    }

                    input_data ={
                        "worker_tasks_list": [task_1]
                    }
                    return input_data
        else:
            raise ValueError(f"favor especificar el stage para continuar ")
    except Exception as e:
        e = str(traceback.format_exc())
        print(e)
        return {"error": e }
