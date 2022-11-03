import os 
from utils.read import read_json_file
base_dir = os.getcwd()
# local_deployment=True
local_deployment=False

""" Environment variables """
if local_deployment== False     :
    SERVICE_NAME =  os.environ['SERVICE_NAME']
    REGION =  os.environ['aws_region']
    STAGE =  os.environ['STAGE']
    S3_BUCKET_DATALAKE =  os.environ['S3_BUCKET_DATALAKE'] 
    PG_TARGET_SCHEMA = os.environ['PG_TARGET_SCHEMA']
    TARGET_DB =  os.environ['TARGET_DB']
    DATALAKE_DB =  os.environ['DATALAKE_DB']
    DATALAKE_CRAWLER =  os.environ['DATALAKE_CRAWLER']
    DELIVERY_PREFIX =  os.environ['DELIVERY_PREFIX']
    CREATE_ATHENA_TABLE_LAMBDA_NAME =  os.environ['CREATE_ATHENA_TABLE_LAMBDA_NAME']
    WAITING_TIME_IN_SECONDS =  os.environ['WAITING_TIME_IN_SECONDS']
    SF_01_NAME_PARALLELIZE_ETLS =  os.environ['SF_01_NAME_PARALLELIZE_ETLS']
    SF_02_NAME_DMS_SERVICE =  os.environ['SF_02_NAME_DMS_SERVICE']
    EXPIRE_URL_SECONDS =  os.environ['EXPIRE_URL_SECONDS']
else:
    base_dir = 'C:\GIT\inputs-estudios\Desarrollo\lambda'
    SERVICE_NAME=   'cannibalization'
    STAGE =   'dev'
    REGION=   'us-east-1'
    S3_BUCKET_DATALAKE =   'georesearch-datalake'
    PG_TARGET_SCHEMA= "aws_migrations"
    TARGET_DB =   'prod_inputs_estudios'
    DATALAKE_DB =   'prod_countries'
    DATALAKE_CRAWLER =   f'dms_{STAGE}_georesearch_datalake'
    DELIVERY_PREFIX =   f'{SERVICE_NAME}_deliveries'
    CREATE_ATHENA_TABLE_LAMBDA_NAME = 'worker-athena-create-table'
    WAITING_TIME_IN_SECONDS = 900
    SF_01_NAME_PARALLELIZE_ETLS =  f"workflow_{STAGE}_inputs_estudios_parallelize_etls"
    SF_02_NAME_DMS_SERVICE =  f"workflow_{STAGE}_dms_replicacion"
    EXPIRE_URL_SECONDS = 60*60*24*5#30*3


"""Construcción de constantes"""

""" Servicio de Athena para crear las tablas"""
CREATE_ATHENA_TABLE_LAMBDA_NAME = f"{SERVICE_NAME}-{STAGE}-{CREATE_ATHENA_TABLE_LAMBDA_NAME}"

""" Tiempo de espera de eventos como crear una tabla o un workflow"""
WAITING_TIME_IN_SECONDS = int(WAITING_TIME_IN_SECONDS)

""" Tipo stage para S3"""
S3_STAGE = 'PROD' #if STAGE in ['prod'] else 'QA'

""" RUTAS DE LAS QUERYS ATHENA """
sql_queries_dir = f"{base_dir}/sql_queries/athena/" 

""" RUTAS DONDE SE GUARDAN LAS TABLAS CREADAS POR LA API """
s3_etl_output_data = f"s3://{S3_BUCKET_DATALAKE}/{S3_STAGE}/athena_processing/{SERVICE_NAME}/"
s3_prefix_etl_output_data = f"/athena_processing/{SERVICE_NAME}/"
s3_prefix_delivery_output_data = f"{S3_STAGE}/athena_processing/{DELIVERY_PREFIX}/"

""" Lista de buffers permitidos """
DEFAULT_BUFFERS = [
      100,
      500,
      600,
      800,
      1000,
      1500,
      2000,
      2500,
      3000,
      3500,
      4000,
      4500,
      5000
    ]

""" Lista de solicitudes desde plaforma MG """
history_customer_list  = read_json_file(f"{base_dir}/utils/history_customer_list.json")

def get_dimanic_sql_path( etl_name , report_name, stage ,history_customer_list =history_customer_list ):
        """
        Retorna el path de la query dependiendo si ( report_name , etl_name y stage ) están en la lista de históricos. 
        IMPORTANTE :
            - El controlador de cada etl no debería ser modificado si se necesita agregar un nuevo customer a la lista , 
        pues se diseño para que independiente de la solicitud, pueda reutilizar la automatización pero con los nuevos cambios de las queries.
            - Obtenerlo por cada stage
            
        Con el fin de ofrecer nuevos requerimientos dentro de las mismas solicitudes y reutilizar lo máximo posible las queries,
        propone la siguiente estructura que sera agregada desde al archivo utils.conf : 
            
            history_customer_list = {
                "customer_one" : {
                    "demografico":{
                        "stages":[1] 
                    },
                    "gastos":{
                        "stages":[1,2]
                    }
                },
                "customer_two" : { 
                    "gastos":{
                        "stages":[1]
                    }
                }
            }
            Esto se lee de la siguiente forma : 
                Para el customer customer_one se tiene que: 
                    El etl demografico fue modificado en/los stage/s 1
                    El etl gastos fue modificado en/los stage/s 1, y 2 
            
            Por lo que los stages reflejaran las nuevas consultas editadas,
            estas deberán ir en la ruta {base_dir}/sql_queries/athena/custom/<report_name>/<etl_names>/0<stage>_<table_name>.sql
            """
        sql_queries_dir = f"{base_dir}/sql_queries/athena/"

        sql_path =  ''
        # si el stage , etl y nombre del customer esta en el historial de solicitudes retorna el path dinamico, sino se utilizan las consultas genericas ya creadas.
        if (
            report_name in history_customer_list.keys() and
            etl_name      in history_customer_list[report_name].keys() and 
            stage         in history_customer_list[report_name][etl_name]['stages']
        ):  
            print(f"Tipo 2️⃣: Consulta dinámica para {report_name}/{etl_name}/ stage {stage}")
            # custom_path :  Query a medida para el reporte o país  :  CUANDO EL FORMATO DE LA SOLICITUD NO COINCIDE CON LA BASE
            sql_path = f"{sql_queries_dir}custom/{report_name}/{etl_name}/"
        else : 
            print(f"Tipo 1️⃣: Consulta genérica para {report_name}/{etl_name}/ stage {stage}")
            # generic_path : Querys genéricas :  CUANDO EL FORMATO DE LAS SOLICITUD COINCIDE PARA CUALQUIER PAÍS O REPORTE 
            sql_path = f"{sql_queries_dir}generic/{etl_name}/"

        return sql_path
