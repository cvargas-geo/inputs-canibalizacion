import os
import boto3 
import traceback 
import awswrangler as wr
from utils.read import read_templated_file
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

def etl_consolidar(event): 
    stage = event.get('stage', None)
    schema = event.get('schema', None)
    report_name = event.get('report_name', None)
    buffer_search = event.get('buffer_search', None)
    drop_table = event.get('drop_workflow', None)
    # id_gastos = event.get('id_gastos', None) #TODO VALIDAR ESTE CAMPO
    parametros = event.get('parametros', None) #Reemplaza a id_gastos
    # etl_name = 'consolidar'
    etl_name = event.get('etl_name', 'consolidar')
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
                    print(" Parte 1 de 1 : Consolidación de los etls")
                    table_name = etl_name 

                    sql_querie  = get_consolidate_table_with_header(
                        report_name  , 
                        schema ,
                        buffer,
                        TARGET_DB,
                        table_name=table_name,
                        etl_name=etl_name,
                        generic_path=generic_path,
                        parametros=parametros
                        ) if drop_table == False else  '--No hay tabla que consultar'
                    assert sql_querie is not None , f"Error al formar la consulta para {etl_name} "

                    custom_table_name = f"{report_name}_{schema}_{table_name}_b{buffer}"
                    print(f"A crear tabla con : {sql_querie}")
                    
                    task_1 = {
                    "task_name": f" Parte 1 de 1 : Consolidación de los etls: {custom_table_name}",
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
                    """Segunda parte generar los archivos csv en el bucket de salida 
                    Copia la tabla consolidada a un bucket de salida haciendo transformaciones de los datos

                    Definición del bucket de salida :
                    
                    {report_name}_inputs
                        {report_name}_{schema}_b{buffer}
                        ...
                        ..
                        .
                    
                    """
                    #IMPORTANTE ESTE PREFIJO DEBE COINCIDIR CON (***)  pues se deben borrar todos los indices si existieran
                    # tbn se ocupa para renombrar los archivos que seran movidos 
                    OUTPUT_FILE_NAME = f"{report_name}_{schema}_b{buffer}_"
                    #TODO S3_STAGE VA DEPENDER DEL ENVIRONMENT
                    custom_s3_output=f"{s3_prefix_delivery_output_data}{report_name}_inputs/{OUTPUT_FILE_NAME}"
                    print(f"⚠️ Se eliminaran los archivos de: {custom_s3_output}"  )
                    s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = custom_s3_output )
                    
                    # elimina todas las salidas csv antes de generar las nuevas 
                    if 'Contents' in s3_response:
                        if len(s3_response["Contents"]) > 0 :
                            print(f"Existen {len(s3_response['Contents'])} archivos por eliminar .... "  ) 
                            for object in s3_response['Contents']:
                                # if f"{report_name}_{schema}_b{buffer}_" in object['Key'] : 
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
                        custom_table_name = f"{report_name}_{schema}_{etl_name}_b{buffer}"
                        source_prefix = s3_prefix_etl_output_data + custom_table_name
                        
                        ##copia la tabla consolidada a un bucket de salida como un archivo .csv.gz cambiando el nombre al archivo
                        print(f"Se copiaran los archivos gz en :{source_prefix}/")
                        # se agrega / pues para la búsqueda b500 retornaba tbn b5000 
                        s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = f"{source_prefix}/"  )
    
                        if 'Contents' in s3_response:
                            if len(s3_response["Contents"]) > 0 :
                                print(f"Existen {len(s3_response['Contents'])} archivos por copiar .... "  ) 
                                # si hay mas de un archivo en el bucket , se agrega un indice al nombre del archivo
                                for index , obj in  enumerate(s3_response["Contents"]):
                                    copy_source = {'Bucket': S3_BUCKET_DATALAKE, 'Key': obj["Key"]}
                                    # (***) Nombre final del archivo de salida
                                    s3_target_key = f"{s3_prefix_delivery_output_data}{report_name}_inputs/{OUTPUT_FILE_NAME}{ index + 1 }.csv.gz"
                                    print(f"Moviendo: {obj['Key']} a {s3_target_key}")
                                    s3_client.copy_object(Bucket = S3_BUCKET_DATALAKE, CopySource = copy_source, Key = s3_target_key)
                            else:
                                print(f"Bucket vacío ✨")
                                raise Exception("Error al eliminar , No puede estar vació el bucket, favor revisar el proceso de consolidación ")
                            
                        result['generar_csv'] = f'✔️ Archivos generados correctamente en: {s3_prefix_delivery_output_data}{report_name}_inputs/'

                        return result
        else:
            raise ValueError(f"favor especificar el stage para continuar ")
    except Exception as e:  
        e = str(traceback.format_exc())
        print(e) 
        return {"error": e }

def get_consolidate_table_with_header( report_name  , schema , buffer_search , TARGET_DB ,table_name,etl_name , generic_path , parametros=None):
    """ retorna una consulta sql que obtiene las columnas las tablas de cada etl anterior,
    y así generar una consulta dinámica. Todas las columnas se castearan a varchar y se agregan en duro las columnas. 
    
    Importante : Solo demografico lleva la columna geo_id el resto de tablas se le omite para evitar duplicado por esta columna
    """
    try:
        
        columnas_demografico= []
        columnas_gastos= []
        columnas_competencias = []
        
        df_demografico = wr.athena.read_sql_query(
                    sql=f"SELECT * FROM {TARGET_DB}.{report_name}_{schema}_demografico_final_b{buffer} LIMIT 0",
                    use_threads =True,
                    database=TARGET_DB
        ) 
        columnas_demografico = list(set(df_demografico.columns))


        if 'id_gastos' in parametros['gastos'][schema]   : 
            df_gastos = wr.athena.read_sql_query(
                        sql=f"SELECT * FROM {TARGET_DB}.{report_name}_{schema}_gastos_final_b{buffer} LIMIT 0",
                        use_threads =True,
                        database=TARGET_DB
            )
            columnas_gastos = list(set(df_gastos.columns))
            columnas_gastos.remove('geo_id') 


        if (   'category_id' in parametros['competencias'][schema]   or 
               'substring_id' in parametros['competencias'][schema]  )  : 
            df_competencias = wr.athena.read_sql_query(
                        sql=f"SELECT * FROM {TARGET_DB}.{report_name}_{schema}_competencias_final_b{buffer} LIMIT 0",
                        use_threads =True,
                        database=TARGET_DB
            )

            columnas_competencias = list(set(df_competencias.columns))
            columnas_competencias.remove('geo_id')       
        
        params = {
            'TARGET_DB': TARGET_DB, 
            'COUNTRY': schema,
            'BUFFER': buffer_search ,
            'report_name':report_name,
            'columnas_demografico' : columnas_demografico,
            'columnas_gastos' : columnas_gastos,
            'columnas_competencias' : columnas_competencias,
            'parametros':parametros
            
        }

        # table_name = 'intersect_blocks_buffers_pois'
        sql_querie = read_templated_file(
            f"{generic_path}01_{table_name}.sql" , 
            params
        )

        #Hox fix for etl conditional logic
        sql_querie  = consolidar_trim_commas(sql_querie)
        
        print(sql_querie)

        return sql_querie
    
    except Exception as e :
        e = str(traceback.format_exc())
        raise Exception(f"❌Error get_consolidate_table_with_header, considera que las tablas pudieron no ser creadas anteriormente {e}")