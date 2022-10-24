#coment if
#   pythonRequirements:
#      layer: true
try:
    import unzip_requirements
except ImportError:
    pass

import os
import json
import uuid
import time
import boto3
import traceback
from random import *
from utils.conf import PG_TARGET_SCHEMA , SF_02_NAME_DMS_SERVICE
from utils import step_functions  as  sf
from utils.db_utils import db_secret, make_conn, execute_query ,get_table_columns
from utils.custom import get_custom_query_for_shape_columns
from utils.glue import add_s3target_to_crawler
from utils.sql_operations import create_table, alter_table, drop_table, update_table
# QUERY_LIMIT = os.environ['QUERY_LIMIT]
# PREFIX_DMS = 'inputs_'
PREFIX_DMS = ''
lambda_client = boto3.client("lambda")

def migrate_table(event, context):
    '''
    WORKER  : Migrates table from one schema to another with shape column as wkt
    '''
    print(event)
    environment = event.get('environment', None)
    SOURCE_SCHEMA = event.get('schema', None)
    SOURCE_TABLE = event.get('table', None)


    conn = make_conn(db_secret[environment])
    query = f"drop table IF exists {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}"
    execute_query(conn, query, {})

    QUERY_LIMIT = os.environ['QUERY_LIMIT']
    table_columns = get_table_columns(conn, SOURCE_SCHEMA , SOURCE_TABLE)
    custom_query = get_custom_query_for_shape_columns( table_columns, SOURCE_SCHEMA , SOURCE_TABLE)
    query = f"create table {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE} as {custom_query}  {QUERY_LIMIT}"

    execute_query(conn, query, {})

    conn.close()
    print(f"✔️ Migrated table  {SOURCE_SCHEMA}.{SOURCE_TABLE} ➡️ {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}")

    #Add new S3 target  to glue crawler
    add_s3target_to_crawler(table_name = f"{SOURCE_SCHEMA}_{SOURCE_TABLE}" , environment=environment )
    # arguments = get_glue_migration_params( country_prefix= SOURCE_SCHEMA , source_table= SOURCE_TABLE)
    # start_glue_job(JobName='job_migracion_postgres',Arguments=arguments)
    time.sleep(10)
    return {
        "status": "OK"
    }


def sync_create_buffer_column(event, context):

    '''
        WORKER  : Migrates tables from one schema to another with shape column as wkt
    '''
    # {'schema': 'mastergeo_countries', 'country': 'cl', 'table': 'view_blocks', 'buffer': 500}

    print(event)
    _schema = event.get('schema', None)
    environment = event.get('environment', None)
    _table = event.get('table', None)
    _buffer= event.get('buffer', None)

    SOURCE_SCHEMA = _schema #f'country_{CUSTOM_COUNTRY}'
    SOURCE_TABLE = _table
    BUFFER_LAYER = _buffer

    conn = make_conn(db_secret[environment])

    # query = f"""ALTER TABLE {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers 
    #             DROP COLUMN IF EXISTS buffer_{BUFFER_LAYER} """
    # print(query)
    # execute_query(conn, query, {})

    query = f"""ALTER TABLE {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers 
                ADD buffer_{BUFFER_LAYER} text NULL"""
    print(query)
    execute_query(conn, query, {})

    # QUERY_LIMIT = os.environ['QUERY_LIMIT']
    # QUERY_LIMIT = ''
    # query = f"""
    # INSERT INTO {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers (buffer_{BUFFER_LAYER})
    #     SELECT 
    #         st_astext(st_buffer(st_setsrid(st_point(longitud, latitud), 4326)::geography, {BUFFER_LAYER})::geometry) as buffer_{BUFFER_LAYER}
    #     FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} {QUERY_LIMIT}
    # """
    query = f"""
    UPDATE {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers A
        SET
            buffer_{BUFFER_LAYER} =  st_astext(st_buffer(st_setsrid(st_point(B.longitud, B.latitud), 4326)::geography, {BUFFER_LAYER})::geometry)
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} B
        WHERE A.id  = B.id
    """

    print(query)
    execute_query(conn, query, {}  )

    conn.close()

    return {
        "status": "OK"
    }


def master_create_buffers(event, context):
    '''
        MASTER  :  Genera los buffers de las tablas especificas de un pais , por lo general se ocupa blocks o view_blocks (incluso view_pois_com_service)
        por cada tabla se genera un buffer en una tabla aparte con el sufijo b_<buffer>, posteriormente se deben unir
    '''
    try:
        event =  event['body']
        # print(event)
        schema_list= event.get('schemas', None)
        environment = event.get('environment', None)
        table_list= event.get('tables', None)
        buffer_list= event.get('buffers', None)
        # schema_list=['country_cl','country_co']
        # table_list=['view_blocks']
        # buffer_list = [500,	600,	800,	1000,	1500,	2000,	2500,	3000,	3500,	4000,	4500,	5000]
        # QUERY_LIMIT  = os.environ['QUERY_LIMIT']
        QUERY_LIMIT  = ' '
        conn = make_conn(db_secret[environment])


        for schema in schema_list :
            for table in table_list :
            
                SOURCE_SCHEMA = schema
                SOURCE_TABLE = table

                """
                For each country, the buffer table is droped and created again
                """
                query = f"DROP TABLE IF EXISTS {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers"
                execute_query(conn, query, {})

                query = f"""
                CREATE TABLE  {PG_TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers AS
                    SELECT
                        id ,
                        block_id,
                        latitud,
                        longitud,
                        administrative_area_level_1,
                        administrative_area_level_2
                    FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}  {QUERY_LIMIT}
                """
                print(query)
                execute_query(conn, query, {})

                #Add new S3 target  to glue crawler
                add_s3target_to_crawler(table_name = f"{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers" , environment=environment )
                
                time.sleep(10)
                for buffer in buffer_list :
                    batch = {
                        'environment': environment,
                        'schema': schema,
                        'table': table,
                        'buffer': buffer
                    }
                    response = lambda_client.invoke(
                                FunctionName="cannibalization-report-dev-create-buffer-column",
                                InvocationType="Event",
                                Payload=json.dumps(  batch  ),
                    )
                    #waits form lambda to finish and bbdd is updated with new buffer column
                    time.sleep(30)

                # time.sleep(10)


        conn.close()

        return {
            "status": "OK"
        }
    except Exception as e:
        e = str(traceback.format_exc())
        return {"status": "FAILED","error_msg": e}

def master_migrate_tables(event,context):
    '''
        MASTER  : Dado una lista de vistas principalmente , 
        estas son migradas al schema de destino su
        reemplazando la columna shape geometry como wkt en caso de tenerlo
    {
    "tables": [{
    "schema": "country_uy",
    "table": "blocks"
    }]

    }
    '''
    event =  event['body']
    environment = event.get('environment', None)
    tables = event.get('tables', None)

    # country_list=['cl','co']
    # table_list=['view_blocks']
    # schema_list=['mastergeo_countries']
    # buffer_list = [500,	600,	800,	1000,	1500,	2000,	2500,	3000,	3500,	4000,	4500,	5000]

    for table in tables:
        print(table)
        table.update({'environment':environment})
        response = lambda_client.invoke(
            FunctionName="cannibalization-report-dev-migrate-table",
            InvocationType="Event",
            Payload=json.dumps(  table  )
    )
    return {
        "status": "OK"
    }


def master_replicate_tables(event,context):
    '''
        MASTER  : Etapa final de la actualización de datos en donde se replican los datos de postgres hacia aws s3 y athena
    --aca json del dms
    '''
    try: 
        print(event )
        event =  event['body']
        rules = event.get('rules', None)
        environment = event.get('environment', None)


        dms_endpoint = {
            "PROD": {
                "SOURCE_ENDPOINT_ARN": "arn:aws:dms:us-east-1:958531303673:endpoint:ASPJENXCI5F6R3JPJFA3TFPS5UUNOKEMWLSDWRA",
                "TARGET_ENDPOINT_ARN": "arn:aws:dms:us-east-1:958531303673:endpoint:dms-prod-migration-datalake"
            },
            "QA": {
                "SOURCE_ENDPOINT_ARN": "arn:aws:dms:us-east-1:958531303673:endpoint:452KHXMN3SCETMQC75H3GZMR4TOJE4FJ35U5CFA",
                "TARGET_ENDPOINT_ARN": "arn:aws:dms:us-east-1:958531303673:endpoint:LXF5MWFWSY7M7US6US4KPDME242KSJSLFBGNOBQ"
            }
        }

        # f = open(f"{base_dir}/resources/step_functions/inputs/01_dms_replication.json" )
        #solicitud básica para el dms
        sf_input = {
    "SKIP": "enablse",
    "SKIP_DELETE": "trues",
    "DEFAULT_WAIT_SECONDS": 120,
    "SOURCE_ENDPOINT_ARN": dms_endpoint[environment]['SOURCE_ENDPOINT_ARN'],
    "TARGET_ENDPOINT_ARN": dms_endpoint[environment]['TARGET_ENDPOINT_ARN'],
    "ReplicationInstanceIdentifier": "dms-replication-instance-for-datalake",
    "ReplicationTaskIdentifier": "dms-replication-task-for-datalake",
    "FILTER_INSTANCE_REPLICATION": [
        "dms-replication-instance-for-datalake"
    ],
    "FILTER_TASK_REPLICATION": [
        "dms-replication-task-for-datalake"
    ],
    "REPLICATION_INSTANCE_PARAMETERS": {
        "ReplicationInstanceIdentifier.$": "$.ReplicationInstanceIdentifier",
        "ReplicationInstanceClass": "dms.c5.4xlarge",
        "ReplicationInstanceClassMin": "dms.t2.micro",
        "AllocatedStorage": 50,
        "EngineVersion": "3.4.6",
        "MultiAZ": False,
        "ReplicationSubnetGroupIdentifier": "default-vpc-f944259d",
        "VpcSecurityGroupIds": [
            "sg-bd8547c4"
        ]
    },
    "MIGRATION_TASK_PARAMETERS": {
        "ReplicationTaskIdentifier.$": "$.ReplicationTaskIdentifier",
        "ReplicationInstanceArn.$": "$.DescribeResult.ReplicationInstances[0].ReplicationInstanceArn",
        "SourceEndpointArn.$": "$.SOURCE_ENDPOINT_ARN",
        "TargetEndpointArn.$": "$.TARGET_ENDPOINT_ARN",
        "MigrationType": "full-load",
        "TableMappings": {
            "rules": [{
                "rule-type": "selection",
                "rule-id": "995195053",
                "rule-name": "995195053",
                "object-locator": {
                    "schema-name": "aws_migrations",
                    "table-name": "country_uy_%"
                },
                "rule-action": "include",
                "filters": []
            }]
        }
    }
}
        #se asignan los nombres unicos respectivos
        seed = str(uuid.uuid1())[:5]
        replication_instance_name = f"dms-replication-task-for-datalake-{seed}"
        replication_task_name = f"dms-replication-task-for-datalake-{seed}"
        sf_input['ReplicationInstanceIdentifier'] = replication_instance_name
        sf_input['ReplicationTaskIdentifier'] = replication_task_name
        sf_input['FILTER_INSTANCE_REPLICATION'] = [replication_instance_name]
        sf_input['FILTER_TASK_REPLICATION'] = [replication_task_name]
        
        #se agregan las reglas de replicación
        # seed = 995195053
        seed = randint(10000, 100000)
        new_rules = []
        if rules : 
            for rule in rules:
                seed = seed + 1
                print(f"Rule: {rule['schema-name']} , {rule['table-name']}")
                new_rule = {
                            "rule-type": "selection",
                            "rule-id": str(seed ),
                            "rule-name": str(seed ),
                            "object-locator": {
                                "schema-name": rule['schema-name'],
                                "table-name": rule['table-name']
                            },
                            "rule-action": "include",
                            "filters": []
                        }   
                new_rules.append(new_rule)
            
            #sobrescribe la reglas con las recibidas
            sf_input['MIGRATION_TASK_PARAMETERS']['TableMappings']['rules'] =  new_rules
        
            print(sf_input)

            """ Nota : Esta step function podría tardar hasta 30 min por lo que,
            se debe implementar un sistema para monitorear el estado de la ejecución por medio de eventos """
            response = sf.iniciar_step_function(SF_02_NAME_DMS_SERVICE ,sf_input )
            execution_arn = response['executionArn']
            time.sleep(6)
            # Se agrega algo de log para la salida de la ejecución
            sf_response = sf.sfn_client.describe_execution(executionArn=execution_arn)
            status = sf_response['status']
            if status == 'FAILED':
                sf_error = sf_response['QueryExecution']['Status']['StateChangeReason']
                raise Exception(sf_error)
            
            return {"status": status,"executionArn": execution_arn} 
        else:
            raise ValueError("❌ rules no debe ser vacío") 

    except Exception as e:
        e = str(traceback.format_exc())
        return {"status": "FAILED","error_msg": e}


def api_secuencial_process_table_buffers(event, context):
    '''
         WRAPPER : Dado una lista de tablas y buffers, genera una lista de jobs para ser ejecutados de forma secuencial
    ''' 
    schema_list= event.get('schemas', None)
    table_list= event.get('tables', None)
    buffer_list= event.get('buffers', None)

    job_list = [] 
    job_id = 0
    for schema in schema_list :
        for table in table_list :
            job_id +=1  
            create_table_op = {"sql_operation":"CREATE TABLE" , "schema" : schema ,  "table" : table ,       "task_id" : job_id }
            job_list.append(create_table_op) 
            for buffer in buffer_list : 
                job_id +=1  
                alter_table_op = {"sql_operation":"ALTER TABLE" , "schema" : schema ,  "table" : table ,   "buffer_size" : buffer ,   "task_id" : job_id }
                job_list.append(alter_table_op)

    payload = {"job_list": job_list}

    response = lambda_client.invoke(
                FunctionName="cannibalization-report-dev-recursive-migrate-tables",
                InvocationType="Event",
                Payload=json.dumps(  payload  ),
    ) 

    return {
        "status": "OK"
    }


def recursive_process_table_buffers(event, context):
    '''
         WRAPPER : Se proesa una lista de Jobs de forma recursiva y secuencial que permitirán ir creando las tablas de buffers junto a sus columnas por separado
    ''' 
    job_list = event.get('job_list', [])
    
    QUERY_LIMIT  = os.environ['QUERY_LIMIT'] 
    PG_TARGET_SCHEMA = PG_TARGET_SCHEMA
    
    if len(job_list) > 0 :
        for job in job_list : # or get first job
    
            if job['sql_operation'] == "CREATE TABLE" :
                
                drop_table(
                    target_schema =PG_TARGET_SCHEMA ,
                    target_table = f"{PREFIX_DMS}{job['schema']}_{job['table']}_buffers" 
                )
                
                create_table(
                    source_schema=job['schema'] , 
                    source_table=job['table']  ,  
                    target_schema=PG_TARGET_SCHEMA, 
                    target_table= f"{PREFIX_DMS}{job['schema']}_{job['table']}_buffers",
                    set_limit = QUERY_LIMIT
                ) 
            elif job['sql_operation'] == "ALTER TABLE" :
                alter_table(
                    target_schema=PG_TARGET_SCHEMA, 
                    target_table= f"{PREFIX_DMS}{job['schema']}_{job['table']}_buffers",
                    buffer_size=int(job['buffer_size'])
                )
                update_table(
                    source_schema=job['schema'] , 
                    source_table=job['table']  ,  
                    target_schema=PG_TARGET_SCHEMA, 
                    target_table= f"{PREFIX_DMS}{job['schema']}_{job['table']}_buffers",
                    buffer_size=int(job['buffer_size'])
                ) 
            else:
                print(f"No se reconoce la operacion {job['sql_operation']}")

            """Finally remove the first job from the list & call the function again"""
            job_list.pop(0)
            if len(job_list) > 0 :
                payload = {"job_list": job_list}
                response = lambda_client.invoke(
                    FunctionName="cannibalization-report-dev-recursive-migrate-tables",
                    InvocationType="Event",
                    Payload=json.dumps(  payload ),
                )
            break
        
        return {
            "status": "OK"
        } 
    else :
        return {
            "status": "VOID"
        }
