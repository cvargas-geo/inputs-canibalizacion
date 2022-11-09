import os 
import boto3
import traceback
import awswrangler as wr 
from utils import business_rules as br
from utils.read import read_templated_file
from utils.conf import CREATE_ATHENA_TABLE_LAMBDA_NAME , DATALAKE_DB,TARGET_DB,get_dimanic_sql_path
from utils.custom import list_to_sql_in
lambda_client = boto3.client("lambda")

def etl_gastos(event ):
    """
    Etapa 1: Crea la intersección de los datos de blocks y buffers , en ese caso es la misma parte de demografico(parte1), se
    decide esto para eliminar la dependencia de este etl , ya que se puede aprovechar el paralelismo.
    Etapa 2: Crea la agregación de los datos de gastos e indices socioeconómicos a partir de la intersección anterior, 
    se debe crea una tabla de agregación por cada canasta incluida el id_gastos(se extrae con otra consulta).
    Etapa 3 : Crea la tabla de gastos final usando las tablas previas de agregación.
    """
    stage = event.get('stage', None)
    schema = event.get('schema', None)
    report_name = event.get('report_name', None)
    buffer_search = event.get('buffer_search', None)
    etl_name = event.get('etl_name', None)
    drop_table = event.get('drop_workflow', None) 
    # id_gastos = event.get('id_gastos', None) #TODO VALIDAR ESTE CAMPO
    parametros = event.get('parametros', None) #Reemplaza a id_gastos
    response = {}
    result = {}
    WAIT_TIME = 3
    worker_tasks_list = [] ##lista de trabajos en paralelo
    
    
    
    try:
        base_dir = os.getcwd()
        sql_queries_dir = f"{base_dir}/sql_queries/athena/"
        
        # id_gastos es obligatorio
        id_gastos = parametros[etl_name][schema]['id_gastos']
        etapa1=True
        etapa2=True
        etapa3=True

        if stage: 
            if stage == 1 :
                #Nota: Se reutiliza la primera parte del etl demografico por lo que se setea etl_name = demografico
                #Nota el generic_path siempre se obtiene a nivel del stage 
                # parametros se envia vacio pues se ocupo en demografico pero aca no 
                generic_path = get_dimanic_sql_path(  'demografico' , report_name, 2)
                if etapa1 :  
                    print(" Parte 1 de 3 : Se hace la intersección y agregación ") 
                    table_name = 'intersect_blocks_buffers'
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                    sql_querie = read_templated_file(
                        f"{generic_path}01_demografico_{table_name}.sql" , 
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'report_name':report_name,
                            'parametros':[]
                        }
                    )
                    worker_parameters = {
                        "task_name": f" Parte 1 de 3 intersección : {table_name}",
                        "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                        "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    } 
                    input_data = {
                        "worker_tasks_list": [worker_parameters]
                    }
                    return input_data

            if stage == 2 :
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                if etapa2 :            
                    """Obtener canastas """
                    if id_gastos and id_gastos is not None :
                        lista_canastas = get_canastas(
                            f"SELECT COALESCE( name , 'NULO') AS canasta  FROM {DATALAKE_DB}.country_{schema}_canasta_categoria where id in ({list_to_sql_in(id_gastos)})"
                        )
                    else:
                        raise ValueError("❌etl_gastos:  No se indicó id_gastos : {id_gastos}")                    # Con la lista de gastos se deben generar un cruce con la tabla de gastos filtrando el gasto específico
                    
                    # cada cruce es una tarea para el worker que realizara en paralelo
                    for i, canasta in enumerate(lista_canastas) :
                        print(f" Parte 2 de 3 : Agregación por canasta : {canasta}")
                        #Importante agregar el nombre de canasta en el nombre de la tabla
                        table_name = f'agregacion'
                        custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_{canasta.lower()}_b{buffer}"
                        sql_querie =''
                        sql_querie = read_templated_file(
                            f"{generic_path}/02_{table_name}.sql" , 
                            {
                                'ETL_NAME': etl_name,
                                'COUNTRY': schema,
                                'BUFFER': buffer_search ,
                                'report_name':report_name,
                                'CANASTA':canasta.lower(),
                                'max_gse':br.get_limit_gse_by_country(schema)
                            }
                        )
                        print(f"sql_querie : {sql_querie}")
                        worker_parameters = {
                            "task_name": f" Parte 2 de 3 agregacion por canasta {i+1} de {len(lista_canastas)} : {table_name}",
                            "worker_parameters": {
                                        "report_name": report_name,
                                        "table_name": custom_table_name, 
                                        "sql_query": sql_querie,
                                        "drop_table": drop_table
                                    },
                            "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                        }
                        
                        worker_tasks_list.append(worker_parameters)

                    input_data = {
                        "worker_tasks_list": worker_tasks_list
                    }
                    return input_data

            if stage == 3 :
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , report_name, stage)
                if etapa3 :  
                    print(" Parte 3 de 3 : Tabla final gastos")
                    """Obtener canastas """
                    if id_gastos and id_gastos is not None :
                        lista_canastas = get_canastas(
                            f"SELECT COALESCE( name , 'NULO') AS canasta  FROM {DATALAKE_DB}.country_{schema}_canasta_categoria where id in ({list_to_sql_in(id_gastos)})"
                        )
                    else:
                        raise Exception("❌etl_gastos:  No se indicó id_gastos : {id_gastos}")                    # Con la lista de gastos se deben generar un cruce con la tabla de gastos filtrando el gasto específico
                    
                    #Nota: Se reutiliza la primera parte del etl demografico
                    table_name = f'final' 
                    custom_table_name = f"{report_name}_{schema}_{etl_name}_{table_name}_b{buffer}"
                
                    sql_querie = read_templated_file(
                        f"{generic_path}03_{table_name}.sql" ,
                        {
                            'ETL_NAME': etl_name,
                            'COUNTRY': schema,
                            'BUFFER': buffer_search ,
                            'TARGET_DB':TARGET_DB,
                            'report_name':report_name,
                            'lista_canastas':lista_canastas,
                            'max_gse':br.get_limit_gse_by_country(schema)
                        }
                    )
                    #reverse string and replace first coma , then reverse string again
                    sql_querie = sql_querie[::-1].replace(",", " ", 1)[::-1]
                    print(f"sql_querie : {sql_querie}")
                    worker_parameters = {
                        "task_name": f" Parte 3 de 3 : Tabla final {table_name}",
                        "worker_parameters": {
                                    "report_name": report_name,
                                    "table_name": custom_table_name, 
                                    "sql_query": sql_querie,
                                    "drop_table": drop_table
                                },
                        "lambda_name":CREATE_ATHENA_TABLE_LAMBDA_NAME
                    } 
                    input_data = {
                        "worker_tasks_list": [worker_parameters]
                    }
                    return input_data
        else:
            raise ValueError(f"favor especificar el stage para continuar ")
    except Exception as e:  
        e = str(traceback.format_exc())
        print(e) 
        return {"error": e } 
    
    
def get_canastas( sql_querie  ):
    try:
        lista_canastas = []
        df_canastas = wr.athena.read_sql_query(
                sql=sql_querie,
                use_threads =True,
                database=DATALAKE_DB
        )
        lista_canastas = [canasta  for canasta in df_canastas['canasta'].tolist()]
        print(f"lista_canastas {len(lista_canastas)}: {lista_canastas}")
        return lista_canastas 
        
    except Exception as e:
        e = str(traceback.format_exc())
        print(e) 
        raise e


# TODO IMPLEMENT
def get_gse_levels(  schema  ):
    try:
        sql_querie = f"""SELECT
                            COUNT(*) AS income_levels 
                            FROM {DATALAKE_DB}.country_{schema}_income_levels"""
        
        lista_canastas = []
        df_canastas = wr.athena.read_sql_query(
                sql=sql_querie,
                use_threads =True,
                database=DATALAKE_DB
        )
        lista_canastas = [canasta  for canasta in df_canastas['canasta'].tolist()]
        print(f"lista_canastas {len(lista_canastas)}: {lista_canastas}")
        return lista_canastas 
        
    except Exception as e:
        e = str(traceback.format_exc())
        print(e) 
        raise e