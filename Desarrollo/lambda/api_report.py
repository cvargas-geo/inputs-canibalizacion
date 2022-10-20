try:
    import unzip_requirements
except ImportError:
    pass
import time
import traceback
import boto3
import json
from utils import athena  as  atn
from utils import step_functions  as  sf
from utils.conf import SF_01_NAME_PARALLELIZE_ETLS , DEFAULT_BUFFERS  ,TARGET_DB , S3_BUCKET_DATALAKE , s3_prefix_delivery_output_data,EXPIRE_URL_SECONDS
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
sf_client = boto3.client('stepfunctions')


def master_report(event, context):
    # start_time = time.time()
    # event = json.loads(event['body'])
    event =  event['body']
    logger.info(f"--->  {event}" )
    log = {}
    customer_list = event.get('customer_list', None)
    environment = event.get('environment', None)

    def validate_request(event ):

        if 'environment' not in  event:
            raise ValueError(f"❌ environment no ha sido especificado")

        customer_list = event['customer_list']
        if customer_list :

            #valida los buffers
            for solicitud in customer_list :
                if solicitud['buffer_list']:
                    for buffer in solicitud['buffer_list']:
                        if buffer not in DEFAULT_BUFFERS :
                            raise ValueError(f"❌ El buffer ingresado [{buffer}] no esta permitido actualmente, Buffers validos son: {DEFAULT_BUFFERS}")
                else:
                    raise ValueError(f"❌ buffer_list no ha sido especificado")

                if 'customer_name' not in  solicitud:
                    raise ValueError(f"❌ customer_name no ha sido especificado")
                if 'country_list' not in  solicitud:
                    raise ValueError(f"❌ country_list no ha sido especificado")
                if 'etl_list' not in  solicitud:
                    raise ValueError(f"❌ etl_list no ha sido especificado")

        else:
            raise ValueError("❌ customer_list no ha sido especificado")

        return True
    
    try:    
        if validate_request(event):
        # if customer_list:

            input_data = {"customer_list":customer_list}

            ## llamada a etls en paralelo
            """ Nota : Esta step function si bien se ejecuta correctamente excede podría tardar hasta 30 min por lo que,
            se debe implementar un sistema para monitorear el estado de la ejecución por medio de eventos """
            response = sf.iniciar_step_function(SF_01_NAME_PARALLELIZE_ETLS ,input_data )
            execution_arn = response['executionArn']

            # Se agrega algo de log para la salida de la ejecución
            sf_response = sf.sfn_client.describe_execution(executionArn=execution_arn)
            status = sf_response['status']
            if status == 'FAILED':
                sf_error = sf_response['QueryExecution']['Status']['StateChangeReason']
                raise Exception(sf_error)

            return {"status": status,"executionArn": execution_arn} 
        # else:
        #     raise ValueError("❌ customer_list no ha sido especificado")


    except Exception as e:
        e = str(traceback.format_exc())
        return {"status": "FAILED","error_msg": e}



#serverless invoke --function worker-athena-create-table --path mocks/report/test.json --stage dev
# serverless invoke --function worker-athena-create-table  --path mocks/create_table/table.json --stage dev
def worker_athena_create_table(event, context):
    start_time = time.time()
    """Esta lambda concurrente sera invocada desde todos los reportes para crear una tabla en athena
    Recordar usar nombres en snake_case sin espacios Ej: mi_tabla_b300
    Y no no deben comenzar con números
    """  
    table_name = event.get('table_name', None)
    sql_query = event.get('sql_query', None) 
    drop_table = event.get('drop_table', False) 

  
    
    #TODO GET FOM ENVIRONMENT VARIABLES
    # SOURCE_DB = 'prod_countries' 
    # TARGET_DB = 'qa_inputs_estudios' 
    # S3_OUTPUT='s3://georesearch-datalake/QA/athena_processing/inputs_estudios/'
    # response = {}
    try:
        # valida que las variables no sean nulas o vacías
        if not [x for x in (table_name,   sql_query) if x  == '' or x is None] :
            
            dll_querie =  atn.create_table(
                table_name   = table_name,  
                target_db    = TARGET_DB , 
                sql_query    = sql_query,
                drop_table   = drop_table 
            ) 
            estado = 'creada' if drop_table == False else 'eliminada'
            # msg = f"✔️ Tabla '{SOURCE_DB}.{table_name}' {estado} con éxito"
            time_remaining = time.time() - start_time
            time_log =  'Time Taken:' +  time.strftime("%H:%M:%S",time.gmtime(time_remaining))
            
            return {
                "status": "OK",
                "dll_querie" : dll_querie,
                "drop_table":drop_table,
                # "mensaje": msg,
                "time_log":time_log
            }
        else:
            raise ValueError(f"❌ Variables no deben ser nulas o vacías {event}")

    except Exception as e:
        return {"status": "FAIL","error": str(e)}
    
    
    
    
def get_download_links(event, context):
    # start_time = time.time()
    # event = json.loads(event['body'])
    event =  event['body']
    logger.info(f"--->  {event}" )
    
    executionArn = event.get('executionArn', None)
    
    # source_prefix = "PROD/athena_processing/georesearch_deliveries"
    source_prefix = s3_prefix_delivery_output_data

    # valida que las variables no sean nulas o vacías
    try:
        if not [x for x in (executionArn ) if x  == '' or x is None]:
        
            response = sf_client.describe_execution(
                executionArn=executionArn
            )

            if response['status'] == 'SUCCEEDED' :  
                
                lista_archivos_por_solicitud = []
                # Rescatando las solicitudes de la ejecución
                sf_input = json.loads(response['input'])
                solicitudes  = sf_input['customer_list']

                # Para generar los enlaces de descarga a s3 se itera por cada solicitud y se rescatan sus archivos desde el bucket S3
                # Las url expiran en x segundos ,ver EXPIRE_URL_SECONDS
                #TODO EXPIRE_URL_SECONDS debería leerse desde serverless.yaml
                for solicitud in solicitudes:

                    customer_name = solicitud['customer_name']
                    # print(solicitud)

                    files_by_solicitud =[] 

                    # consulta por los archivos en el bucket correspondiente de la solicitud ,
                    # recordar que se le agrego "_inputs" al final de customer_name al momento de guardar 
                    # source_prefix ya lleva /
                    s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = f"{source_prefix}{customer_name}_inputs/" )
                    print(S3_BUCKET_DATALAKE  , f"{source_prefix}/{customer_name}_inputs/", s3_response  )
                    # un orden de la salida
                    files = sorted(s3_response['Contents'],key=lambda i:i['Key'],reverse=True)  

                    # print(s3_response)
                    # files = s3_response['Contents']

                    #por cada archivo dentro del bucket se genera una url prefirmada que solo es visible por quien tenga este enlace
                    url_list = []

                    for file in files : 
                        file_obj = {} # detalle de cada archivo , nombre y url 

                        #obtiene el ultimo split correspondiente al nombre 
                        file_name  = file['Key'].split('/')[-1]

                        # split anterior corresponde al nombre de la solicitud  
                        # input_name  = file['Key'].split('/')[-2]

                        file_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': S3_BUCKET_DATALAKE, 'Key':file['Key']},
                            ExpiresIn=EXPIRE_URL_SECONDS)

                        # se genera el detalle por archivo y se concatena a la lista final por solicitud
                        file_obj = {
                            'name':file_name,
                            'url':file_url 
                        }
                        url_list.append(file_obj)

                    # result = {'url_list':url_list}

                    #aca se genera el detalle por cada solicitud junto a sus urls
                    lista_archivos_por_solicitud.append({'customer_name':customer_name,'url_list':url_list})
                    
                
                # print(json.dumps(lista_archivos_por_solicitud ,  indent=4) )
                return lista_archivos_por_solicitud

            else:
                raise "Error, esta ejecución no esta lista para ser consultada."
        else:
            raise ValueError(f"Error (executionArn), No deben ser vacíos: {event}")

    except Exception as e:
        return {"status": "FAIL","error": str(e)}