# Script para generar el input del lambda de mirgacio de tablas de inputs estudios

selected_countries = [ 
    'country_pe' ,
      'country_cl' , 
#     'customer_geolab_mx',
    'country_co' , 
    'country_mx'  
]
tables= [
#     'blocks' ,  
#     'pois', 
    'pois_comercios_servicios_view'   ,
    'view_gastos', 
    'view_blocks', 
    "empresas",
    "categories",
    "income_levels"
]
# bd_name = 'mastergeo_countries'
limit = ' '
migracion_tablas = { 
    "tables": [ 
        {
#             "bd_name": bd_name,
            "schema": country,
            "table": table,
            
        } for table in tables for country in selected_countries
    ]   
} 
import json 
print(json.dumps(  migracion_tablas , indent=4)) 
json_requests = json.dumps(  migracion_tablas , indent=4)