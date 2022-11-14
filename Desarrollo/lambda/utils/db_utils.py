import os
import select
import psycopg2
from psycopg2.extras import RealDictCursor
from .aws_utils import get_secret

import pandas as pd
import awswrangler as wr
# import sqlalchemy
# from sqlalchemy import create_engine, MetaData
from utils.conf import (
    S3_BUCKET_DATALAKE ,
    s3_prefix_etl_output_data,
    SERVICE_NAME
    )

stage = os.environ["stage"]
aws_region = os.environ["aws_region"]
# db_secret = get_secret(f"{stage}/mastergeo/db" , aws_region)


db_secret = {
    "PROD": {
        "dbname": "mastergeo_countries",
        "host": "10.10.0.40",
        "password": "mDrZ~*=X",
        "username": "postgres",
        "port": 5432,
    },
    "QA": {
        "dbname": "mastergeo_countries",
        "host": "10.10.0.52",
        "password": "mDrZ~*=X",
        "username": "postgres",
        "port": 5432,
    }
}


def wait(conn): 
    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)


def make_conn( db_secret):
    # print("DEBUG: Begin make_conn")
    # stage = os.environ["stage"]
    # aws_region = os.environ["aws_region"]
    # db_secret = get_secret("%s/mastergeo/db" % (stage), aws_region)
    db_host = db_secret["host"]
    db_name = db_secret["dbname"]
    db_user = db_secret["username"]
    db_pass = db_secret["password"]

    conn = None
    stringConn = "dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pass)
    try:
        # print("Conectando a la BD con '%s'" % (stringConn))
        conn = psycopg2.connect(stringConn)
    except ValueError:
        print("Database connection problem")

    # print("DEBUG: End make_conn")
    return conn
def make_async_conn(db_secret):
    """note that async conection can not be use commit or autocomit true""" 
    db_host = db_secret["host"]
    db_name = db_secret["dbname"]
    db_user = db_secret["username"]
    db_pass = db_secret["password"]

    aconn = None
    stringConn = "dbname='%s' user='%s' host='%s' password='%s'" % (db_name, db_user, db_host, db_pass)
    try:
        print("Conectando a la BD con '%s'" % (stringConn))
        aconn = psycopg2.connect(stringConn , async_ =True)
        wait(aconn)
    except ValueError:
        print("Database connection problem")

    return aconn


def fetch_data(conn, query, params, format = None):
    # print("DEBUG: Begin fetch_data with query : '%s' params '%s'" % (query, params))
    cursor = conn.cursor(cursor_factory=RealDictCursor) if format == 'json' else conn.cursor()
    cursor.execute(query, params)
    result = cursor.fetchall()
    # print("DEBUG: End fetch_data")
    return result

def execute_query(conn, query, params):
    print(f"🏁 Execute: {query} , {params}" )
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    # print("DEBUG: End execute_query")

def async_execute_query(aconn, query, params):
    # print("DEBUG: Begin execute_query with query : '%s' params '%s'" % (query, params))
    acursor = aconn.cursor()
    acursor.execute(query, params)
    wait(acursor.connection)
    # print("DEBUG: End execute_query")

def get_table_columns(conn, schema , table_name  ) : 
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {schema}.{table_name} LIMIT 0" )
    colnames = [desc[0] for desc in cursor.description]
    cursor.close()
    return colnames

#TODO implement async_fetch_data

def postgres_to_athena(table_name , environment , event , custom_schema_prefix = 'customer_' , columns='*'):
    """Ejecuta una consulta a postgres para traer table_name y cargarla en athena,
        Idealmente pensado para tablas pequeñas < 100 mb
        Borra columnas nulas si existen en el df por incompatibilidad con awswrangler
        Se utiliza el input enviado al api para guardar los datos en S3"""
    if environment.upper() in ["PROD","QA"]:
        conn = make_conn(db_secret[environment.upper()])
        report_name = event['report_name']
        pais = event['schema']
        report_name = event['report_name']
        sql = f"SELECT {columns} FROM {custom_schema_prefix}{report_name}_{pais}.{table_name};"
        print(sql)
        # solo pra el caso de chedraui comentar
        # sql = f"""SELECT  * FROM customer_{report_name}_{schema}.locales_propiosvf3;"""

        df = pd.read_sql(sql, conn)

        # por paz mental se borran las columnas nulas
        df=df.dropna(axis=1,how='all')

        conn.close()
        rows , cols = df.shape[0] ,df.shape[1]
        print(f"Copiando datos de {table_name}  {rows},{cols}")
        target_s3_url = f"s3://{S3_BUCKET_DATALAKE}/{environment.upper()}{s3_prefix_etl_output_data}{report_name}_{pais}_{table_name}/"
        wr.s3.to_parquet(
            df=df,
            path=target_s3_url,
            dataset=True,
            index=False,
            sanitize_columns = False,
            use_threads=True,
            mode="overwrite",
            database=f"{environment.lower()}_{SERVICE_NAME}",
            table=f'{report_name}_{pais}_{table_name}'
        )
        del df
        ##validación de datos
        df = wr.athena.read_sql_query(
            f"SELECT * FROM {environment.lower()}_{SERVICE_NAME}.{report_name}_{pais}_{table_name}",
            database=f"{environment.lower()}_{SERVICE_NAME}"
        )
        if rows != df.shape[0] or cols != df.shape[1] :
            raise Exception(f"Las dimensiones no coinciden, al copiar los datos de {table_name} , {df.shape}")
    else:
        raise Exception("Bad string environment given")

    return True
