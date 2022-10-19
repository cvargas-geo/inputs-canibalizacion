import traceback
import boto3
import json
from botocore.exceptions import ClientError
from utils.conf import S3_BUCKET_DATALAKE , DATALAKE_DB ,  DATALAKE_CRAWLER , STAGE

glue_client = boto3.client('glue')

def delete_a_table_from_database(database_name, table_name):
   """ Retorna True si la tabla fue eliminada """
   operation_state = False
   try:
      if custom_get_table(database_name, table_name):
         response = glue_client.delete_table(DatabaseName= database_name, Name = table_name  )
         print(f"Eliminando tabla de glue {database_name}, {table_name} , {response}")
         assert response['ResponseMetadata']['HTTPStatusCode'] == 200 , "Error HTTPStatusCode al eliminar la tabla en glue"
         #ojo aca valida que no exista
         assert custom_get_table(database_name, table_name) == False , "Error la tabla todavía existe"
         print(f"✔️ Tabla '{database_name}.{table_name}' eliminada ,{response}")
         return True
      else:
         print(f"⚠️ Tabla '{database_name}.{table_name}' no existe, nada que eliminar ")
         return True

   except ClientError as e:
      print(f"⚠️ ClientError delete_a_table_from_database :{str(e) }")
      return False 
   except Exception as e:
      print(f"⚠️ Exception delete_a_table_from_database  {str(e) }")
      return False 
   
   




def check_table_exist(database_name , table_name) :
   """Valida que la tabla exista entes de eliminarla"""
   table_state = False
   try:
      response = glue_client.get_table( 
         DatabaseName=database_name,
         Name=table_name
      )
      table_state  = True if response['Table']['Name'] == table_name else False 
      print(f"✔️ Table exist '{database_name}.{table_name}'  {table_state}") 
      return table_state 

   except glue_client.exceptions.EntityNotFoundException as e:
      print(f"⚠️ EntityNotFoundException: {str(e) }")
      return False 
   except Exception as e:
      print(f"⚠️ Exception {str(e) }") 
      return False  
   
   # return False 

def custom_get_table(database_name , table_name):
    try:
        paginator = glue_client.get_paginator('get_tables')
        page_iterator = paginator.paginate(
            DatabaseName=database_name 
        )
        for page in page_iterator:
            # print(len(page['TableList']))
            for table in page['TableList'] : 
                # print(table['Name'])
                if table_name == table['Name']:
                    return True

        return False
    
    except glue_client.exceptions.EntityNotFoundException as e:
        print(f"⚠️ EntityNotFoundException: {str(e) }")
        return False 
    except Exception as e:
        print(f"⚠️ Exception {str(e) }") 
        return False  



# table_name = 'test_table'
# database_name = 'inputs_estudioss'
# custom_get_table(database_name , table_name)




def start_glue_job(JobName=None, Arguments={}):
    
    glue_client = boto3.client('glue', region_name='us-east-1')
    try:
        response = glue_client.start_job_run(
            JobName=JobName,
            Arguments=Arguments
        )
        print(response)
    except Exception as e:
        print(e)
        pass


def get_glue_migration_params( country_prefix=None, source_table=None, ):
    """ Se generan los parametros para el job de migracion de datos """

    country_prefix=country_prefix#'country_cl'
    source_table=source_table#'categories'

    #TODO DEFINIR LAS CONSTANTES COMO VARIABLES DE ENTORNO

    # mastergeo_countries/customer_geolab_cvargas

    source_glue_db='qa_mapeo_migracion_postgres'
    source_S3_datalake='s3://georesearch-datalake/demo_datalake/raw/'
    target_glue_db='qa_migracion_postgres'
    # target_glue_table=source_table

    """
        El nombre de la tabla mapeada la crea el crawler por defecto
        Se compone por el nombre de la base de datos - schema - tabla
    """
    source_db='mastergeo_countries'          # db por defecto
    source_schema='customer_geolab_cvargas'  #schema por definir
    crawler_source_table = f"{source_db}_{source_schema}_{country_prefix}_{source_table}"

    arguments = {
            '--SOURCE_TABLE'  : crawler_source_table,
            '--SOURCE_DB'     : source_glue_db,
            '--SOURCE_BUCKET' : f"{source_S3_datalake}{crawler_source_table}/",

            '--TARGET_GLUE_DB': target_glue_db,
            '--TARGET_TABLE'  : f"{country_prefix}_{source_table}"
    }
    print(arguments)
    return arguments



def add_s3target_to_crawler(table_name , environment=None  ):
    """ Agrega un nuevo target a un crawler si es que no existe, este crawler es genérico por stage dev-qa y prod """
    try:
        
        DINAMIC_STAGE = "dev" if environment in ['QA'] else "prod"
        CRAWLER_NAME=f"dms_{DINAMIC_STAGE}_georesearch_datalake"
        DINAMIC_BD_STAGE = "qa" if environment in ['QA'] else "prod"
        BD_NAME = f"{DINAMIC_BD_STAGE}_countries"
        
        crawler_details = glue_client.get_crawler(Name= CRAWLER_NAME)
        S3TargetsList = crawler_details['Crawler']['Targets']['S3Targets']

        new_table_name = table_name
        new_S3TargetsList = []

        new_S3TargetsList = S3TargetsList
 
        if new_table_name not in str(S3TargetsList) :

            new_s3_target_path = f"s3://{S3_BUCKET_DATALAKE}/{environment}/aws_migrations/{new_table_name}/" 
            new_S3TargetsList.append({'Path': new_s3_target_path })

            response = glue_client.update_crawler(
                Name=CRAWLER_NAME,
                # Role=crawler_role,
                DatabaseName=BD_NAME,
                Targets={
                    'S3Targets': new_S3TargetsList
                }
            )
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200 , "No se pudo actualizar el crawler"
            print(f"✔️ Se agrega un nuevo s3 target : {new_s3_target_path}")
            # Nota : El crawler se inicia en un paso posterior cuando instancia de replicación DMS termine


    except ClientError as e:
        e = str(traceback.format_exc())
        print(f"❌ Error add_s3target_to_crawler  {e}")