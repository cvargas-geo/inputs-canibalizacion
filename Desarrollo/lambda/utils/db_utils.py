import os
import select 
import psycopg2
from psycopg2.extras import RealDictCursor
from .aws_utils import get_secret

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
