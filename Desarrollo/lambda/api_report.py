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
from utils.response import response_error, response_ok
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
sf_client = boto3.client('stepfunctions')


def input_validation(event):
    """ Valida el input de entrada y aplica las regla para los parametros necesarios de los etls
    retorna status y el mensaje de error en caso de algun error"""
    #all params´s inputs
    base_params = ["reports_request"]
    request_params = ["environment" , "report_name" ,  "schema"  ,"report_to" , "drop_workflow" , "etl_list" ,"parametros"] 
    etl_list = ["local","delivery","captura"]

    #implementado
    local_params_etl=["buffer_search" ,"pois_state_id","surface_factor" ,"distance_factor","canasta_categoria_id","substring_id","pois_category_id"]
    delivery_params_etl=["cannibalization_shape","canasta_categoria_id","substring_id","pois_category_id"]

    #por implementar
    captura_params_etl=["similar_a_local_pues_ocupa_precalculo_pero_con_zonas"]
    gap_params_etl=["cap_param"]

    def missing_parameter(parameter   ):
        return False, f"Se espera '{parameter}'"# Se crea el objeto de la clase

    # por cada input se validan todos sus parametros
    for base_param in base_params :
        if base_param not in  event :
            return missing_parameter(f"{base_param}")# si el parametro no esta en el request, no se valido

        for request in event.get(base_param) :
            # print(request)

            for request_param in request_params :
                if request_param not in  request :
                    return missing_parameter(f"{base_param}.{request_param}")

            if len( request.get('etl_list')) == 0 :

                return  False , "Debe especificar almenos un etl"

#           Reglas, cada etl en etl_list debe constener sus parametros dentro de la clave parametros
            default_params_name = "parametros"
#             se itera por los etls de la request para validar si existen sus parametros respectivos
            for etl in request.get('etl_list'):
                if etl not in  request.get(default_params_name) :
                    return missing_parameter(f"{default_params_name}.{etl}")

                # si las claves existen, se validan los parametros particulares por cada etl
                if etl == 'local' :
                    for etl_param in local_params_etl :
                        if etl_param not in  request.get(default_params_name).get(etl) :
                            return missing_parameter(f"{default_params_name}.{etl}.{etl_param}")
                if etl == 'delivery' :
                    for etl_param in delivery_params_etl :
                        if etl_param not in  request.get(default_params_name).get(etl) :
                            return missing_parameter(f"{default_params_name}.{etl}.{etl_param}")
                if etl == 'captura' :
                    for etl_param in captura_params_etl :
                        if etl_param not in  request.get(default_params_name).get(etl) :
                            return missing_parameter(f"{default_params_name}.{etl}.{etl_param}")
                if etl == 'gap' :
                    for etl_param in gap_params_etl :
                        if etl_param not in  request.get(default_params_name).get(etl) :
                            return missing_parameter(f"{default_params_name}.{etl}.{etl_param}")
    return True , _



def report(event, context):
    """En este caso cada request corresponde a una solicitud ,
        pero se podría transformar en una lista de solicitudes para procesos masivos
    """    # start_time = time.time()
    # event = json.loads(event['body'])
    event =  event['body']
    logger.info(f"--->  {event}" )

    # def validate_request( event ):
    #     """Valida todas las solicitudes antes de procesarlas"""
    #     reports_request = event.get('reports_request', None)
    #     base_required_params = ["environment" , "report_name","schema","report_to","drop_workflow","parametros"]
    #     if reports_request :
    #         for solicitud in reports_request :
    #             if 'environment' not in  solicitud:
    #                 raise ValueError(f"❌ environment no ha sido especificado")
    #             if 'report_name' not in  solicitud:
    #                 raise ValueError(f"❌ report_name no ha sido especificado")
    #             if 'schema' not in  solicitud:
    #                 raise ValueError(f"❌ schema no ha sido especificado")
    #             if 'report_to' not in  solicitud:
    #                 raise ValueError(f"❌ report_to no ha sido especificado")
    #             if 'drop_workflow' not in  solicitud:
    #                 raise ValueError(f"❌ drop_workflow no ha sido especificado")
    #             if 'buffer_search' not in  solicitud:
    #                 raise ValueError(f"❌ buffer_search no ha sido especificado")
    #             if 'surface_factor' not in  solicitud:
    #                 raise ValueError(f"❌ surface_factor no ha sido especificado")
    #             if 'distance_factor' not in  solicitud:
    #                 raise ValueError(f"❌ distance_factor no ha sido especificado")
    #             if 'start_point' not in  solicitud:
    #                 raise ValueError(f"❌ start_point no ha sido especificado")
    #             if 'cannibalization_shape' not in  solicitud:
    #                 raise ValueError(f"❌ cannibalization_shape no ha sido especificado")
    #             # if 'substring_id' not in  solicitud:
    #             #     raise ValueError(f"❌ substring_id no ha sido especificado")
    #             if 'pois_category_id' not in  solicitud:
    #                 raise ValueError(f"❌ pois_category_id no ha sido especificado")
    #             if 'etl_list' not in  solicitud:
    #                 raise ValueError(f"❌ etl_list no ha sido especificado")
    #     else:
    #         raise ValueError("❌ reports_request no ha sido especificado")

    #     return True

    try:

        is_valid , message = input_validation(event)
        if is_valid(event):
            reports_request = event.get('reports_request', None)

            # if reports_request :
            #     for solicitud in reports_request :


            input_data = {"reports_request":reports_request}
            status = 'TEST'
            execution_arn = 'fake:arn:12345678'
            if True :
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
        else:
            return message

    except Exception as e:
        e = str(traceback.format_exc())
        return  response_error(e)



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
    db_stage = event.get('db_stage', False) 

    #TODO GET FOM ENVIRONMENT VARIABLES
    # SOURCE_DB = 'prod_countries' 
    # TARGET_DB = 'qa_inputs_estudios' 
    # S3_OUTPUT='s3://georesearch-datalake/QA/athena_processing/inputs_estudios/'
    # response = {}
    try:
        # valida que las variables no sean nulas o vacías
        if not [x for x in (table_name,   sql_query , db_stage) if x  == '' or x is None] :

            dll_querie =  atn.create_table(
                table_name   = table_name,
                target_db    = TARGET_DB ,
                sql_query    = sql_query,
                drop_table   = drop_table,
                db_stage = db_stage
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
                solicitudes  = sf_input['reports_request']

                # Para generar los enlaces de descarga a s3 se itera por cada solicitud y se rescatan sus archivos desde el bucket S3
                # Las url expiran en x segundos ,ver EXPIRE_URL_SECONDS
                #TODO EXPIRE_URL_SECONDS debería leerse desde serverless.yaml
                for solicitud in solicitudes:

                    report_name = solicitud['report_name']
                    # print(solicitud)

                    files_by_solicitud =[] 

                    # consulta por los archivos en el bucket correspondiente de la solicitud ,
                    # recordar que se le agrego "_inputs" al final de report_name al momento de guardar 
                    # source_prefix ya lleva /
                    s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = f"{source_prefix}{report_name}_inputs/" )
                    print(S3_BUCKET_DATALAKE  , f"{source_prefix}/{report_name}_inputs/", s3_response  )
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
                    lista_archivos_por_solicitud.append({'report_name':report_name,'url_list':url_list})
                    
                
                # print(json.dumps(lista_archivos_por_solicitud ,  indent=4) )
                return lista_archivos_por_solicitud

            else:
                raise "Error, esta ejecución no esta lista para ser consultada."
        else:
            raise ValueError(f"Error (executionArn), No deben ser vacíos: {event}")

    except Exception as e:
        return {"status": "FAIL","error": str(e)}