import os 
import boto3
import traceback
from utils.read import read_templated_file
from utils.conf import CREATE_ATHENA_TABLE_LAMBDA_NAME , get_dimanic_sql_path

lambda_client = boto3.client("lambda")

def etl_demografico(event):
    stage = event.get('stage', None)
    schema = event.get('schema', None)
    report_name = event.get('report_name', None)
    buffer_search = event.get('buffer_search', None)
    etl_name = event.get('etl_name', None)
    drop_table = event.get('drop_workflow', None)
    parametros = event.get('parametros', None) #TODO VALIDAR ESTE CAMPO
    response = {}
    result = {} 
    
    try:
        base_dir = os.getcwd()
        sql_queries_dir = f"{base_dir}/sql_queries/athena/" 

        if stage: 
            if stage == 1 :
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                do=True
                if do :  
                    print(""" Parte 1 de 4 : Se hace la intersección y agregación """)
                    table_name = 'intersect_blocks_buffers'
                    sql_querie = read_templated_file(
                        f"{generic_path}01_{etl_name}_{table_name}.sql" , 
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'report_name':report_name,
                            'parametros':parametros['demografico'][schema]
                        }
                    )
                    
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                    task_1 = {
                      "task_name": f" Parte 1 de 4 : {table_name}",
                      "worker_parameters": {
                                "report_name": report_name,
                                "table_name": custom_table_name, 
                                "sql_query": sql_querie,
                                "drop_table": drop_table
                            },
                      "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    } 
                do=True
                if do :
                    print(""" Parte 2 de 4 : Se hace la intersección y agregación""")
                    
                    table_name = 'intersect_pois_buffers'
                    sql_querie = read_templated_file(
                        f"{generic_path}02_{etl_name}_{table_name}.sql" , 
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'report_name':report_name,
                            'parametros':parametros['demografico'][schema]
                        }
                    )
                    
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                    
                    task_2 = {
                          "task_name": f" Parte 2 de 4  : {table_name}",
                          "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                          "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                        } 
     
                do=True
                if do :
                    print(""" Parte 3 de 4 : Se hace la intersección y agregación""")
                    table_name = 'intersect_empresas_buffers'
                    sql_querie = read_templated_file(
                        f"{generic_path}03_{etl_name}_{table_name}.sql" , 
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'report_name':report_name,
                            'parametros':parametros['demografico'][schema]
                            
                        }
                    )
                    
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                    task_3 = {
                          "task_name": f" Parte 3 de 4   : {table_name}",
                          "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                          "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                        } 
                
                input_data ={
                  "worker_tasks_list": [task_1,task_2 ,task_3 ]
                }  
                return input_data
                
            if stage == 2 :
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                do=True
                if do :
                    print(""" Stage 2 de 2 : Consulta final de datos demográficos""")
                    
                    #Se agregan aca los atractores
                    atractor_rule = read_templated_file(
                        f"{generic_path}atractor_rule.{schema}.sql"   
                    )
                    
                    table_name = 'final'
                    sql_querie = read_templated_file(
                        f"{generic_path}04_{table_name}.sql" , 
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'report_name':report_name,
                            'atractor_rule':atractor_rule
                        }
                    )
                    
                    # no se coloca el nombre_etl pues ya es el ultimo paso
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                    task_4 = {
                          "task_name": f" Stage 2 de 2   : {table_name}",
                          "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                          "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                        }  
                input_data ={
                  "worker_tasks_list": [task_4 ]
                }  
                return input_data
    
                '''             
                if stage == 3:
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                do=True
                if do :
                    print(""" Parte 4 de 4 : Consulta final de datos demográficos""")
                    
                    table_name = 'demografico'
                    sql_querie = read_templated_file(
                        f"{generic_path}04_{table_name}.sql" , 
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'report_name':report_name
                        }
                    )
                    
                    # no se coloca el nombre_etl pues ya es el ultimo paso
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_b{buffer}"
                    task_4 = {
                        "task_name": f" Parte 4 de 4   : {table_name}",
                        "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                        "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                        }
                    input_data ={
                        "worker_tasks_list": [ task_4 ]
                    }
                    return input_data

                    '''

        else:
            raise ValueError(f"favor especificar el stage para continuar ")

    except Exception as e:
        e = str(traceback.format_exc())
        print(e) 
        raise e