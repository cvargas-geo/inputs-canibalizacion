import re

def shape_in_columns(table_columns, shape_column='shape') : 
    """Retorna true si el elemento existe en la lista """
    #https://stackoverflow.com/a/68262291/4450951
    return True if True in list(map(lambda col : shape_column in col ,table_columns)) else False

assert True  == shape_in_columns(['a','asd','sd', 'shape'] ) , 'Test Fail'
assert False == shape_in_columns(['a','asd','sd'         ] ) , 'Test Fail'


def get_custom_query_for_shape_columns(table_columns, schema , table_name  , set_limit = False) :
    """ If shape column is present in table , add 2 transformed columns as wkt  """
    query = "SELECT\n"
    SET_LIMIT = f' LIMIT {set_limit}' if set_limit is not False else ''
    print("Shape is present ? : " , shape_in_columns( table_columns ))

    if shape_in_columns( table_columns ) :
        table_columns.remove('shape')
        for column in table_columns :
            query = query + f"    {column} , \n"
        # add shape as  string
        # add centroid as string
        query = query + f"""    st_astext(shape) AS shape_wkt,\n    st_astext(st_centroid(shape)) AS centroid_wkt \n"""
    else :
        for column in table_columns :
            query = query + f"    {column} , \n"
        #  fix last comma
        query = re.sub(r'(,)[^,]*$', '',query)+'\n'


    #Fix some MAYUSC columns only for mx 
    query = (query
             .replace('OBJECTID' , '"OBJECTID"')
             .replace('tipoCenCom' , '"tipoCenCom"')
             .replace('nom_CenCom' , '"nom_CenCom"')
             .replace('tipoUniEco' , '"tipoUniEco"')
             .replace('EMPLEADOS' , '"EMPLEADOS"')
             .replace('CATEGORIA' , '"CATEGORIA"')
             )
    # query = query.replace('EMPLEADOS' , '"EMPLEADOS"')

    query = query + f"FROM {schema}.{table_name}  {SET_LIMIT}"
    print(query)
    return query

def list_to_sql_in(python_list): 
    """Convert a list to a sql IN statement like (1,2,3) """
    return ','.join(map(str, python_list))


def list_replace(lista ,old, new ):
    return list(map(lambda x: x.replace(old, new), lista))

def consolidar_trim_commas(sql):
    """ Elimina la ultima coma de cada parte del union all """
    sql = sql.replace('\n\n','\n').replace('\n\n','\n') 
    parts = sql.split('/*split_for_comma*/')
    #reverse string and replace first coma , then reverse string again
    p1 = parts[0][::-1].replace(",", " ", 1)[::-1]
    p2 = parts[1][::-1].replace(",", " ", 1)[::-1]
    p3 = parts[2]
    return p1+p2+p3