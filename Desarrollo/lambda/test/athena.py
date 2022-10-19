import sys
fun_name = lambda n=0: sys._getframe(n + 1).f_code.co_name

from utils import athena  as  atn  

def test_create_table(): 
    table_name = 'test' 
    target_db = 'inputs_estudios'
    sql_query = """SELECT 
                    * , 0 as geo_id
                    FROM prod_countries.country_cl_empresas 
                    limit 10;"""

    assert atn.create_table(table_name, target_db, sql_query) == True , f"Error  {fun_name()}, {locals()}"



def test_delete_table(): 
    table_name = 'test' 
    target_db = 'inputs_estudios'
    sql_query = 'SELECT * , 0 as geo_id FROM prod_countries.country_cl_empresas limit 10;'

    assert atn.create_table(table_name, target_db, sql_query, drop_table=True) == True , f"Error  {fun_name()}, {locals()}"
    