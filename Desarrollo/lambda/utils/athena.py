# import awswrangler as wr 
# import traceback
import time
import boto3
from botocore.exceptions import ClientError
import pandas as pd
# import awswrangler as wr
import sqlalchemy
from sqlalchemy import create_engine, MetaData
from utils.db_utils import make_conn, execute_query
from utils.conf import (
    S3_BUCKET_DATALAKE ,
    s3_prefix_etl_output_data,
    SERVICE_NAME
    )


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# boto3.setup_default_session(profile_name="tecnologia")
from utils.read import read_templated_file
from utils.conf import sql_queries_dir , s3_prefix_etl_output_data,S3_BUCKET_DATALAKE, WAITING_TIME_IN_SECONDS,SERVICE_NAME
from utils.glue import glue_client,  delete_a_table_from_database
from utils import step_functions as sf  
s3_client_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

def create_table(table_name, target_db , sql_query  , drop_table=False  , db_stage=None ):
    """ Wrapper para crear/eliminar una tabla en athena sino existe , realiza los sgtes pasos
        1 :  elimina la tabla del glue catalog 
        2 :  elimina los datos de s3 s3_output
        3 :  crea nuevamente la tabla en el glue catalog y los datos quedan alojados en s3
        Retorna True si todo salio bien
        drop_table :  si es True , solo elimina la tabla

    """
    try :
        # valida que las variables no sean nulas o vacías
        if not [x for x in (locals()) if x  == '' or x is None] :

            assert delete_table( f"{db_stage}_{target_db}" , table_name )  == True , "Error al eliminar la tabla del glue catalog"
            assert delete_s3_data( table_name ,db_stage=db_stage)  == True , "Error al eliminar los datos de tabla en s3"
            dll_querie = ''
            TABLE_FORMAT ='csv'
            logger.info(f"⚽ DDL DROP MODE  : {drop_table}")
            logger.info(f"⚽ TABLE FORMAT: {TABLE_FORMAT}")


            if drop_table == False :
                s3_etl_output_data = f"s3://{S3_BUCKET_DATALAKE}/{db_stage.upper()}/athena_processing/{SERVICE_NAME}/"
                logger.info(f"🚀 A ejecutar : \n{sql_query}")
                dll_querie = read_templated_file(
                    file_path=f'{sql_queries_dir}/generic/create_table/create_{TABLE_FORMAT}_table.sql',
                    params={
                        'table_name':table_name ,
                        'target_db':f"{db_stage}_{target_db}" ,
                        'sql_query': sql_query ,
                        's3_table_location':f"{s3_etl_output_data}{table_name}/"
                    }
                )
                logger.info(f"🚴 API Running : \n{dll_querie}")

                """
                    OPCION 1 : Ejecuta la consulta en athena con boto
                        Errores : Esporádicamente la consulta indica que la tabla ya existe, indicando que no se borro completamente la tabla en s3 , pero el log indica que si .

                    OPCION 2 : Ejecuta la consulta en athena con Step Functions
                        Errores : el lambda etl que ejecuta el proceso pierde conexión con el worker , 
                        pues la step function se ejecuta duplicadamente, perdiendo la traza y dando un timeout

                    Notar que si solo se especifica el WorkGroup no es necesario indicar el OutputLocation
                """
                # option_for_execute_query = 2
                # if option_for_execute_query == 1 : 
                #     response = athena_client.start_query_execution(
                #         QueryString=dll_querie,
                #         WorkGroup='primary' 
                #         # ResultConfiguration={
                #         #     'OutputLocation':  s3_etl_output_data+'metadata/'+table_name+'/',
                #         #     }
                #         )
                #     assert  wait_create_table(response['QueryExecutionId']) == True , "Error al crear la tabla"

                # if option_for_execute_query == 2 :  
                #     sf_response = sf.iniciar_step_function(
                #         'workflow_execute_athena_querie' ,
                #         {
                #             "myQueryString" : dll_querie,
                #             "WorkGroup" : 'primary' ,
                #         # "OutputLocation" : s3_etl_output_data+'metadata/'+table_name+'/' ,
                #         }
                #     )
                #     execution_arn = sf_response['executionArn'] 
                #     assert sf.wait_step_function(execution_arn )  == True , "Error al crear la tabla"

                logger.info("✔️ DDL creada con éxito" )
            
            # si los casos terminaron con exito ...
            estado = 'creada' if drop_table == False else 'eliminada'
            msg = f"✔️ Tabla {db_stage}_{target_db}.{table_name} {estado} con éxito"
            logger.info(msg)
            return dll_querie
        
        else:
            raise ValueError(f'Variables no deben ser nulas o vacías {locals()}')
        # response['ctas_query_metadata']#['Status']['State']
    except ClientError as e:
        # e = traceback.format_exc()
        logger.info(f'ClientError create_table   :  {str(e)}')
        return str(e)
    except Exception as e:
        # e = traceback.format_exc()
        logger.info(f'Exception create_table    :  {str(e)}')
        return str(e)



# deprecado
def create_table_original(table_name, target_db , sql_query  , drop_table=False  ):
    """ Wrapper para crear/eliminar una tabla en athena sino existe , realiza los sgtes pasos
        1 :  elimina la tabla del glue catalog 
        2 :  elimina los datos de s3 s3_output
        3 :  crea nuevamente la tabla en el glue catalog y los datos quedan alojados en s3
        Retorna True si todo salio bien
        drop_table :  si es True , solo elimina la tabla
    """
    try :
        # valida que las variables no sean nulas o vacías
        if not [x for x in (locals()) if x  == '' or x is None] :

            assert delete_table( target_db , table_name )  == True , "Error al eliminar la tabla del glue catalog"
            assert delete_s3_data( table_name )  == True , "Error al eliminar los datos de tabla en s3"

            TABLE_FORMAT ='csv'
            logger.info(f"⚽ DDL DROP MODE  : {drop_table}") 
            logger.info(f"⚽ TABLE FORMAT: {TABLE_FORMAT}") 
            if drop_table == False :
                logger.info(f"🚀 A ejecutar : \n{sql_query}") 
                dll_querie = read_templated_file(
                    file_path=f'{sql_queries_dir}/generic/create_table/create_{TABLE_FORMAT}_table.sql', 
                    params={
                        'table_name':table_name ,
                        'target_db':target_db ,
                        'sql_query': sql_query ,
                        's3_table_location':f"{s3_etl_output_data}{table_name}/"
                    }
                )
                logger.info(f"🚴 API Running : \n{dll_querie}")

                """
                    OPCIÓN 1 : Ejecuta la consulta en athena con boto
                        Errores : Esporádicamente la consulta indica que la tabla ya existe, indicando que no se borro completamente la tabla en s3 , pero el log indica que si .

                    OPCIÓN 2 : Ejecuta la consulta en athena con Step Functions
                        Errores : el lambda etl que ejecuta el proceso pierde conexión con el worker , 
                        pues la step function se ejecuta duplicadamente, perdiendo la traza y dando un timeout

                    Notar que si solo se especifica el WorkGroup no es necesario indicar el OutputLocation
                """
                option_for_execute_query = 2
                if option_for_execute_query == 1 :
                    response = athena_client.start_query_execution(
                        QueryString=dll_querie,
                        WorkGroup='primary'
                        # ResultConfiguration={
                        #     'OutputLocation':  s3_etl_output_data+'metadata/'+table_name+'/',
                        #     }
                        )
                    assert  wait_create_table(response['QueryExecutionId']) == True , "Error al crear la tabla"

                if option_for_execute_query == 2 :
                    sf_response = sf.iniciar_step_function(
                        'workflow_execute_athena_querie' ,
                        {
                            "myQueryString" : dll_querie,
                            "WorkGroup" : 'primary' ,
                        # "OutputLocation" : s3_etl_output_data+'metadata/'+table_name+'/' ,
                        }
                    )
                    execution_arn = sf_response['executionArn']
                    assert sf.wait_step_function(execution_arn )  == True , "Error al crear la tabla"

                logger.info("✔️ DDL Ejecutada con éxito" )

            # si los casos terminaron con exito ...
            estado = 'creada' if drop_table == False else 'eliminada'
            msg = f"✔️ Tabla {target_db}.{table_name} {estado} con éxito"
            logger.info(msg)
            return True

        else:
            raise ValueError(f'Variables no deben ser nulas o vacías {locals()}')
        # response['ctas_query_metadata']#['Status']['State']
    except ClientError as e:
        # e = traceback.format_exc()
        logger.info(f'ClientError create_table   :  {str(e)}')
        return False
    except Exception as e:
        # e = traceback.format_exc()
        logger.info(f'Exception create_table    :  {str(e)}')
        return False



def wait_create_table(QueryExecutionId):
    """ Espera a que la tabla se cree en athena """
    # iterations = 360 # 30 mins max of athena execution time
    ITERATIONS = 0 #
    # WAITING_TIME_IN_SECONDS
    WAIT_TIME = 15
    status = "RUNNING"
    while  ITERATIONS < WAITING_TIME_IN_SECONDS:
        ITERATIONS += WAIT_TIME
        if ITERATIONS % WAIT_TIME == 0:
            logger.info(f"⏱️ {ITERATIONS} , Esperando que la consulta termine {QueryExecutionId} ... {status}" )
        time.sleep(WAIT_TIME)

        response_get_query_details = athena_client.get_query_execution(
            QueryExecutionId = QueryExecutionId
        )
        status = response_get_query_details['QueryExecution']['Status']['State']

        if (status == 'FAILED') or (status == 'CANCELLED') :
            failure_reason = response_get_query_details['QueryExecution']['Status']['StateChangeReason']
            logger.info(f"🔥 Error al ejecutar querie, razón: {failure_reason} {response_get_query_details}"  )
            return False
        elif status == 'RUNNING':
            continue
        elif status == 'SUCCEEDED':
            # location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
            logger.info(f'✔️ DDL Ejecutada con éxito')
            return True 

    logger.info(f"💀 Consulta supero los {WAITING_TIME_IN_SECONDS/60} min max de ejecución : {QueryExecutionId}") 
    return False



def delete_table(target_db , table_name):
    """ Elimina una tabla de athena (wrapper de glue ) """
    try :
        logger.info("Deleting table from glue catalog")
        if delete_a_table_from_database( target_db , table_name ) :
            logger.info(f"✔️ Tabla eliminada  '{target_db}.{table_name}'")
            return True
        else:
            raise Exception(f"Error al eliminar la tabla '{target_db}.{table_name}'")
    except Exception as e:
        # e = traceback.format_exc()
        logger.info( f"Error al eliminar la tabla '{target_db}.{table_name}' Error : {str(e)}"  )
        return False

def delete_s3_data(table_name , db_stage = None ):
    """Elimina los datos relacionados a la tabla indicada dentro del datastore de inputs estudios """
    try:
        # custom_s3_output=f'{s3_prefix_etl_output_data}{table_name}/'
        # logger.info(f"Deleting table from s3 bucket : {custom_s3_output}"  )
        # bucket = s3_client_resource.Bucket(S3_BUCKET_DATALAKE)
        # bucket.objects.filter(Prefix=custom_s3_output).delete()
        # # time.sleep(5)
        # logger.info(f"Deleting table from s3 bucket :{s3_prefix_etl_output_data}metadata/{table_name}/tables"  )
        # bucket.objects.filter(Prefix=f"{s3_prefix_etl_output_data}metadata/{table_name}/tables").delete() #ONLY METADATA

        custom_s3_output=f'{db_stage.upper()}{s3_prefix_etl_output_data}{table_name}/'
        logger.info(f"Deleting table from s3 bucket :{custom_s3_output}"  )
        s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = custom_s3_output )

        if 'Contents' in s3_response:
            if len(s3_response["Contents"]) > 0 :
                logger.info(f"Quedan {len(s3_response['Contents'])} archivos por eliminar .... "  )
                for object in s3_response['Contents']:
                    print('Deleting', object['Key'])
                    s3_client.delete_object(Bucket=S3_BUCKET_DATALAKE, Key=object['Key'])

        # custom_s3_output=f'{s3_prefix_etl_output_data}metadata/{table_name}/tables'
        # logger.info(f"Deleting table from s3 bucket :{custom_s3_output}"  )
        # s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = custom_s3_output )

        # if 'Contents' in s3_response:
        #     if len(s3_response["Contents"]) > 0 :
        #         logger.info(f"Quedan {len(s3_response['Contents'])} archivos por eliminar .... "  )
        #         for object in s3_response['Contents']:
        #             print('Deleting', object['Key'])
        #             s3_client.delete_object(Bucket=S3_BUCKET_DATALAKE, Key=object['Key'])
        #     assert   len(s3_response["Contents"]) == 0 , "Error al eliminar los datos de la tabla"
        assert   wait_for_delete_files(table_name, metadata=False , db_stage = db_stage) == True , "❌Error esperando eliminar los datos de la tabla"
        # assert   wait_for_delete_files(table_name, metadata=True) == True , "❌Error esperando eliminar los metadatos de la tabla"

        logger.info(f'✔️ Datos de la tabla "{table_name}" en el bucket {custom_s3_output} eliminados')
        return True
    except Exception as e:
        # e = traceback.format_exc()
        # e = str(e)
        logger.info(f'❌ Error al eliminar los datos de la tabla "{table_name}" en el bucket {custom_s3_output} , error :{str(e)} ')
        return False


def wait_for_delete_files(table_name, metadata=False,db_stage = None ):
    """Esta funcion elimina la metadata creada para la tabla de athena , si no se elimina no deja crear una nueva tabla """

    ITERATIONS = 0
    WAIT_TIME = 3
    # status = "Waiting"

    custom_s3_output=f'{db_stage.upper()}{s3_prefix_etl_output_data}{table_name}/'

    if metadata :
        custom_s3_output=f'{s3_prefix_etl_output_data}metadata/{table_name}/tables'
    else:
        custom_s3_output=f'{s3_prefix_etl_output_data}{table_name}/'

    while  ITERATIONS < WAITING_TIME_IN_SECONDS:
        ITERATIONS += WAIT_TIME
        if ITERATIONS % WAIT_TIME == 0:
            print(f"⏱️ {ITERATIONS} , Esperando eliminar de  {custom_s3_output}  ")

        time.sleep(WAIT_TIME)

        s3_response = s3_client.list_objects_v2(Bucket= S3_BUCKET_DATALAKE, Prefix = custom_s3_output )

        if 'Contents' in s3_response:
            if len(s3_response["Contents"]) > 0 :
                logger.info(f"Quedan {len(s3_response['Contents'])} archivos por eliminar ....{s3_response['Contents']}"  )
                for object in s3_response['Contents']:
                    print('Deleting', object['Key'])
                    s3_client.delete_object(Bucket=S3_BUCKET_DATALAKE, Key=object['Key'])

                continue

            elif len(s3_response["Contents"]) == 0 :
                return True

        elif 'Contents' not in s3_response :
            logger.info(f"✔️Sin datos en el bucket  : {custom_s3_output}"  )
            return True



def athena_to_postres(df , schema , table_name ,credential=None   ):
    try:
        engine = create_engine(f"postgresql+psycopg2://{credential['username']}:{credential['password']}@{credential['host']}:{credential['port']}/{credential['dbname']}")
        meta = MetaData(engine, schema=schema)
        meta.reflect(engine, schema=schema)
        pdsql = pd.io.sql.SQLDatabase(engine, meta=meta)

        # df = pd.read_sql("SELECT * FROM xxx", con=engi    ne)
        # https://stackoverflow.com/questions/71970584/pandas-to-sql-with-dict-raises-cant-adapt-type-dict-is-there-a-way-to-avoi
        pdsql.to_sql(df, table_name , if_exists='replace' , index=False , dtype={"properties": sqlalchemy.types.JSON})

        # aprovecho de convertir el shape texto a geometry y borrar la columna shape_wkt
        if "shape_wkt" in df.columns :
            conn = make_conn(credential)

            sql_querie = f"""ALTER TABLE {schema}.{table_name}
                            ADD COLUMN shape geometry;"""
            execute_query(conn  , sql_querie , {})
            sql_querie = f"""UPDATE {schema}.{table_name}
                            SET shape = ST_GeomFromText(shape_wkt);"""
            execute_query(conn  , sql_querie , {})
            sql_querie = f"""ALTER TABLE  {schema}.{table_name}
                            DROP COLUMN shape_wkt;"""
            execute_query(conn  , sql_querie , {})

            conn.close()

    except Exception as e:
        print("Error al copiar la tabla con sqlalchemy", e)
        raise Exception(f"Error al copiar la tabla con sqlalchemy", e)