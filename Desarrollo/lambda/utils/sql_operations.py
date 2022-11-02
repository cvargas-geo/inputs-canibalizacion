from utils.db_utils import db_secret, make_conn,  execute_query 

def drop_table( target_schema, target_table ):
    """
    Drop a table in the database, for a new Job
    """
    conn = make_conn(db_secret)
    # query = f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.{PREFIX_DMS}{SOURCE_SCHEMA}_{SOURCE_TABLE}_buffers"
    query = f"DROP TABLE IF EXISTS {target_schema}.{target_table}"
    execute_query(conn, query, {}) 
    conn.close()
    print(f"✔️ Successfully dropped table {target_schema}.{target_table}")

def create_table( source_schema, source_table  ,  target_schema, target_table ,set_limit = ''):
    """
    Create a table in the database, for a new Job
    """
    conn = make_conn(db_secret)
    
    custom_columns = get_custom_columns_for_table(source_table)
    
    query = f"""
    CREATE TABLE  {target_schema}.{target_table} AS
        SELECT {custom_columns}
        FROM {source_schema}.{source_table}  {set_limit}
    """
    print(query)
    execute_query(conn, query, {}) 
    conn.close()
    print(f"✔️ Successfully created table {target_schema}.{target_table}")


def alter_table(target_schema, target_table  ,buffer_size):
    """
    Alter a table in the database, add text column with buffer.
    """
    conn = make_conn(db_secret) 
    query = f"ALTER TABLE {target_schema}.{target_table} ADD buffer_{buffer_size} text NULL"
    execute_query(conn, query, {})
    conn.close()
    print(f"✔️ Successfully altered table {target_schema}.{target_table}")
    
    
def update_table(source_schema, source_table  ,  target_schema, target_table , buffer_size):
    """
    Update buffer_search column of table in the database.
    """
    conn = make_conn(db_secret)
    custom_on_clause = get_custom_on_clause_for_table(source_table) 
    custom_buffer = get_custom_buffer_for_table(source_table ,buffer_size) 
    query = f"""
    UPDATE {target_schema}.{target_table} A
        SET buffer_{buffer_size} =  {custom_buffer}
        FROM {source_schema}.{source_table} B
        WHERE {custom_on_clause} -- like a  A.id  = B.id
    """
    execute_query(conn, query, {})
    conn.close()
    print(f"✔️ Successfully updated table {target_schema}.{target_table}")


def get_custom_columns_for_table(table_name ):
    """
    Return a custom columns given a table to be inclued in te buffer_search table .
    """
    if table_name == 'view_blocks' : 
        custom_columns = """
                    id ,
                    block_id,
                    latitud,
                    longitud,
                    administrative_area_level_1,
                    administrative_area_level_2
                    """
                    
    if table_name == 'pois' : 
        custom_columns = """id_pois ,id """
        
    if table_name == 'view_bla' : 
        custom_columns = """bla ,bla """
        
        
    return custom_columns


def get_custom_on_clause_for_table(table_name ):
    """
    Return a custom on clause given a table to be inclued in te buffer_search table .
    """
    if table_name == 'view_blocks' : 
        custom_on_clause = """
                    A.id = B.id
                    """
                    
    if table_name == 'pois' : 
        custom_on_clause = """A.id_pois = B.id_pois """
        
    if table_name == 'view_bla' : 
        custom_on_clause = """A.bla = B.bla """
         
    return custom_on_clause


def get_custom_buffer_for_table(table_name , buffer_size ):
    """
    Return a custom buffer_search given a table to be inclued in te buffer_search table .
    Some tables have a custom coords by lat long or direcly a shape or point 
    """
    if table_name == 'view_blocks' : 
        custom_buffer = f"""
                    st_astext(st_buffer(st_setsrid(st_point(B.longitud, B.latitud), 4326)::geography, {buffer_size})::geometry)
                    """
    if table_name == 'pois' : 
        custom_buffer = f"""st_astext(st_buffer(B.shape::geography, {buffer_size})::geometry) """
        
    if table_name == 'view_bla' : 
        custom_buffer = f"""st_astext(st_buffer( , {buffer_size})::geometry) """

    return custom_buffer