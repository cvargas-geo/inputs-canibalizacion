import os 
import boto3
import traceback
from utils.custom import list_to_sql_in
from utils.read import read_templated_file
from utils.conf import (
    CREATE_ATHENA_TABLE_LAMBDA_NAME , 
    DATALAKE_DB, TARGET_DB , 
    S3_BUCKET_DATALAKE, 
    s3_prefix_delivery_output_data,
    s3_prefix_etl_output_data,
    s3_etl_output_data,
    get_dimanic_sql_path
    )

s3_client = boto3.client('s3')

def etl_locales_propios(event ):
    stage = event.get('stage', None)
    country_name = event.get('country_name', None)
    customer_name = event.get('customer_name', None)
    buffer = event.get('buffer', None)
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
            if stage == 1 and etapa1 :
                #Nota el generic_path siempre se obtiene a nivel del stage  
                generic_path = get_dimanic_sql_path(  etl_name , customer_name, stage)
                task_name = f" Parte 1 de 1 : Se hace la intersección y agregación "
                print(task_name)
                #Nota: Se reutiliza la primera parte del etl demografico
                table_name = 'get_locales'
                sql_querie = read_templated_file(
                    f"{generic_path}01_{table_name}.sql" , 
                    {
                        'ETL_NAME': etl_name,
                        'COUNTRY': country_name,
                        'BUFFER': buffer ,
                        'CUSTOMER_NAME':customer_name,
                        'parametros':parametros[etl_name][country_name],
                        'search_distance_in_meters':5000
                    }
                )
                # print("___________")
                # print(sql_querie)
                # print("___________")
                
                custom_table_name = f"{customer_name}_{country_name}_{etl_name}_{table_name}"
                
                worker_parameters = {
                    "task_name": task_name,
                    "worker_parameters": {
                                "customer_name": customer_name,
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
                if etapa2 :   
                    """Segunda parte generar los archivos csv en el bucket de salida 
                    Copia la tabla get_locales a un bucket de salida haciendo transformaciones de los datos

                    Definición del bucket de salida :
                    
                    {customer_name}_inputs
                        {customer_name}_{country_name}_b{buffer}
                        ...
                        ..
                        .
                    
                    """
                    #IMPORTANTE ESTE PREFIJO DEBE COINCIDIR CON (***)  pues se deben borrar todos los indices si existieran
                    # tbn se ocupa para renombrar los archivos que seran movidos 
                    OUTPUT_FILE_NAME = f"{customer_name}_{country_name}_locales_propios_"
                    
                    custom_s3_output=f"{s3_prefix_delivery_output_data}{customer_name}_inputs/{OUTPUT_FILE_NAME}"
                    print(f"⚠️ Se eliminaran los archivos de: {custom_s3_output}"  )
                    s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = custom_s3_output )
                    
                    # elimina todas las salidas csv antes de generar las nuevas 
                    if 'Contents' in s3_response:
                        if len(s3_response["Contents"]) > 0 :
                            print(f"Existen {len(s3_response['Contents'])} archivos por eliminar .... "  ) 
                            for object in s3_response['Contents']:
                                # if f"{customer_name}_{country_name}_b{buffer}_" in object['Key'] : 
                                print('🗑️ Eliminando : ', object['Key'])
                                s3_client.delete_object(Bucket=S3_BUCKET_DATALAKE, Key=object['Key'])
                    else:
                        print(f"Bucket vacío ✨")
                        
                    if drop_table :
                        result['eliminar_csv'] = f'✔️ Archivos finales {custom_s3_output} eliminados correctamente' 
                        return result
                    else  :
                        #TABLA A CONSULTAR
                        #dummy_customer_pe_consolidar_b500
                        custom_table_name = f"{customer_name}_{country_name}_{etl_name}_get_locales"
                        source_prefix = s3_prefix_etl_output_data + custom_table_name
                        
                        ##copia la tabla consolidada a un bucket de salida como un archivo .csv.gz cambiando el nombre al archivo
                        print(f"Se copiaran los archivos gz en :{source_prefix}")
                        s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = source_prefix )
    
                        if 'Contents' in s3_response:
                            if len(s3_response["Contents"]) > 0 :
                                print(f"Existen {len(s3_response['Contents'])} archivos por copiar .... "  ) 
                                # si hay mas de un archivo en el bucket , se agrega un indice al nombre del archivo
                                for index , obj in  enumerate(s3_response["Contents"]):
                                    copy_source = {'Bucket': S3_BUCKET_DATALAKE, 'Key': obj["Key"]}
                                    # (***) Nombre final del archivo de salida
                                    s3_target_key = f"{s3_prefix_delivery_output_data}{customer_name}_inputs/{OUTPUT_FILE_NAME}{ index + 1 }.csv.gz"
                                    print(f"Moviendo: {obj['Key']} a {s3_target_key}")
                                    s3_client.copy_object(Bucket = S3_BUCKET_DATALAKE, CopySource = copy_source, Key = s3_target_key)
                            else:
                                print(f"Bucket vacío ✨")
                                raise Exception("Error al eliminar , No puede estar vació el bucket, favor revisar el proceso de consolidación ")
                            
                        result['generar_csv'] = f'✔️ Archivos generados correctamente en: {s3_prefix_delivery_output_data}{customer_name}_inputs/'

                        return result

        else:
            raise Exception(f"favor especificar el stage para continuar ")

    except Exception as e:  
        e = str(traceback.format_exc())
        print(e) 
        return {"error": e } 
