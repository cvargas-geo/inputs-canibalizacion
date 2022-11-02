import os 


base_dir = os.getcwd()

sql_queries_dir = f"{base_dir}/sql_queries/athena/" 

## 1 ) Querys genéricas :  CUANDO EL FORMATO DE LAS TABLAS COINCIDE PARA CUALQUIER PAIS O REPORTE 
generic_path = f"{sql_queries_dir}/generic/{etl_name}/"
## 2 ) Query a medida para el reporte o país  :  CUANDO EL FORMATO DE LAS TABLAS COINCIDE ENTRE LOS PAISES 
custom_path = f"{sql_queries_dir}/custom/{report_name}/{etl_name}/"


