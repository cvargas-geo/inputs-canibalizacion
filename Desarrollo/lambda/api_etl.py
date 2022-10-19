try:
    import unzip_requirements
except ImportError:
    pass
import boto3

from etls.demografico import etl_demografico
from etls.gastos import etl_gastos
from etls.competencias import etl_competencias
from etls.consolidar import etl_consolidar
from etls.locales_propios import etl_locales_propios
from utils import step_functions  as  sf 

lambda_client = boto3.client("lambda")

"""
    {
  "customer_list": [
    {
      "customer_name": "dummy_customer",
      "country_list": [
        "pe"
      ],
      "buffer_list": [
        600 
      ],
      "id_gastos": [
        21,
        15
      ],
      "etl_list": [
        "demografico" 
      ]
    } 
  ]
}
"""
def handler( event, context):
    print(f"event --->: {event}")
    etl_name = event.get('etl_name', None)
    customer_name = event.get('customer_name', None)
    country_name = event.get('country_name', None)
    buffer = event.get('buffer', None)
    parametros = event.get('parametros', None) #TODO VALIDAR ESTE CAMPO
    # drop_table,drop_workflow = event.get('drop_workflow', False) 
    response = {}
    
    try:
    
      # valida que las variables no sean nulas o vacías
      if not [x for x in (country_name,customer_name ,etl_name , buffer, parametros) if x  == '' or x is None] :

        if etl_name == "demografico":
            response = etl_demografico(event)
        
        if etl_name == "gastos":
            response = etl_gastos(event) 
            
        if etl_name == "competencias":
            response = etl_competencias(event)
            
        if etl_name == "locales_propios":
            response = etl_locales_propios(event)
            
        if etl_name == "consolidar":
            response = etl_consolidar(event) 
        
        
        return {"status": "OK","response": response}
      else:
          raise ValueError(f"Error (country_name,customer_name ,etl_name , buffer, parametros), No deben ser vacíos: {event}")

    except Exception as e: 
        # tb = traceback.format_exc()
        response['error'] = str(e)  
        return {"status": "FAIL","response": response}