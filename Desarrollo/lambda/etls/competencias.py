import os 
import boto3
import traceback
import awswrangler as wr
from utils.custom import list_to_sql_in
from utils import step_functions  as  sf 
from utils.read import read_templated_file
from utils.conf import (
    CREATE_ATHENA_TABLE_LAMBDA_NAME , 
    DATALAKE_DB, TARGET_DB,
    get_dimanic_sql_path
    )

lambda_client = boto3.client("lambda")

def etl_competencias(event ):
    stage = event.get('stage', None)
    schema = event.get('schema', None)
    report_name = event.get('report_name', None)
    buffer_search = event.get('buffer_search', None)
    etl_name = event.get('etl_name', None)
    drop_table = event.get('drop_workflow', None) 
    parametros = event.get('parametros', None) #TODO VALIDAR ESTE CAMPO
    response = {}
    result = {}
    WAIT_TIME = 3
    worker_tasks_list = [] ##lista de trabajos en paralelo



    try:
        base_dir = os.getcwd()
        sql_queries_dir = f"{base_dir}/sql_queries/athena/" 

        etapa1=True
        etapa2=True
        #esto se coloca para que cuando elimine la lambda worker no falle al validar este parametro de entrada
        sql_querie='/*Nada que ejecutar*/'

        if stage: 
            if stage == 1 :
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                if etapa1 :  
                    task_name = f" Parte 1 de 4 : Se hace la intersección y agregación "
                    print(task_name)
                    #Nota: Se reutiliza la primera parte del etl demografico
                    table_name = 'intersect_blocks_buffers_pois'
                    sql_querie = read_templated_file(
                        f"{generic_path}01_{table_name}.sql" , 
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'report_name':report_name,
                            'parametros':parametros['competencias'][schema]
                        }
                    )
                    # print("___________")
                    # print(sql_querie)
                    # print("___________")
                    
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                    
                    worker_parameters = {
                        "task_name": task_name,
                        "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                        "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    } 
                    return {
                        "worker_tasks_list": [worker_parameters]
                    }
        
            if stage == 2 :
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                if etapa2 :   
                    task_name = f"Parte 2 de 2 : Se hacen los pivotes ssa sba y c  reemplazan espacios por _ , es tabla final  "
                    print(task_name)
                    table_name = 'final'
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                    nombre_tabla_anterior = f"{TARGET_DB}.{report_name}_{schema}_{etl_name}_intersect_blocks_buffers_pois_b{buffer}"
                    
                    if not drop_table : 
                        ##Obtener subcadenas para los pivotes , el if es por que la tabla ya estara borrada si drop_table = True 
                        lista_subcadenas = get_subcadenas( 
                            f"SELECT DISTINCT COALESCE(subcadena, 'NULO') AS subcadena FROM {nombre_tabla_anterior}"  
                        ) 

                        sql_querie = read_templated_file(
                            f"{generic_path}02_{table_name}.sql" , 
                            {
                                'ETL_NAME': etl_name,
                                'COUNTRY': schema,
                                'BUFFER': buffer_search ,
                                'report_name':report_name,
                                'parametros':parametros['competencias'][schema],
                                'lista_subcadenas':lista_subcadenas,
                                'nombre_tabla_anterior':nombre_tabla_anterior
                            }
                        ) 
                    #aunque quiera borrar se retorna lo mismo, pero da igual no se ocupara
                    worker_parameters = {
                        "task_name": task_name,
                        "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                        "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    } 
                    return {
                        "worker_tasks_list": [worker_parameters]
                    }

        else:
            raise ValueError(f"favor especificar el stage para continuar ")

    except Exception as e:  
        e = str(traceback.format_exc())
        print(e) 
        return {"error": e } 


def get_subcadenas( sql_querie  ):
    try:
        lista_subcadenas = []
        df_subcadenas = wr.athena.read_sql_query(
                sql=sql_querie,
                use_threads =True,
                database=DATALAKE_DB
        )
        lista_subcadenas = [subcadena  for subcadena in df_subcadenas['subcadena'].tolist()]
        print(f"lista_subcadenas {len(lista_subcadenas)}: {lista_subcadenas}")
        return lista_subcadenas 
        
    except Exception as e:
        e = str(traceback.format_exc())
        print(e) 
        raise e