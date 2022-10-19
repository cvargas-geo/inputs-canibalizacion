import json 
from jinja2 import Template , StrictUndefined
#doc util
#https://jinja.palletsprojects.com/en/2.10.x/templates/#assignments
def read_file(file_path) : 
    """Solo lee un archivo de texto dado su path"""
    return open(file_path ,"r").read()


def read_templated_file(file_path  , params = {} ) :
    """ Dada una ruta de archivo de texto y un diccionario de parametros, 
    retorna una consulta con los parámetros reemplazados
     doc util:
     https://jinja.palletsprojects.com/en/2.10.x/templates/#assignments
     Notas útiles en Jinja2 No es posible declarar variables dentro de sentencias for :/
    """
    # print("Read sql querie")
    file = read_file(file_path)
    # print(file)
    #, undefined=StrictUndefined avisa si una variable no esta definida
    templed_sql = Template(file , undefined=StrictUndefined).render(params=params)
    # print(templed_sql)
    return templed_sql.replace("\n", " ").replace("\t", " ")



def read_json_file(file_path):
    with open(file_path) as f:
        return json.load(f)

    #  f = open(file_path) 
    # # Reading from file
    # data = json.loads(f.read())
    # return data