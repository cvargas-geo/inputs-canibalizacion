try:
    import unzip_requirements
except ImportError:
    pass
import boto3

# from etls.demografico import etl_demografico
from etls.local import etl_local
# from etls.local import etl_delivery

from utils import step_functions  as  sf

lambda_client = boto3.client("lambda")

"""
{
  "reports_request": [
    {
      "environment": "PROD",
      "report_name": "ficcus",
      "schema": "cl",
      "report_to": [
        "cvargas@georesearch.cl"
      ],
      "drop_workflow": false,
      "buffer_search": 2000,
      "pois_state_id": 1,
      "surface_factor": -1.95,
      "distance_factor": 1.1,
      "start_point": "POINT(X,X)",
      "cannibalization_shape": "POLYGON((X,X))",
      "canasta_categoria_id": [
        34
      ],
      "substring_id_o_subcadena": [
        4,
        5,
        6
      ],
      "pois_category_id": [
        10008
      ],
      "etl_list": [
        "local",
        "delivery"
      ]
    }
  ]
}
"""
def handler( event, context):
    print(f"event --->: {event}")
    etl_name = event.get('etl_name', None)
    report_name = event.get('report_name', None)
    schema = event.get('schema', None)
    buffer_search = event.get('buffer_search', None)
    parametros = event #.get('parametros', None) #TODO VALIDAR ESTE CAMPO
    # drop_table,drop_workflow = event.get('drop_workflow', False)
    response = {}

    try:

      # valida que las variables no sean nulas o vacías
      if not [x for x in (schema,report_name ,etl_name , buffer_search ) if x  == '' or x is None] :

        # if etl_name == "demografico":
        #     response = etl_demografico(event)

        if etl_name == "local":
            response = etl_local(event)

        # if etl_name == "delivery":
        #     response = etl_delivery(event)

        # if etl_name == "competencias":
        #     response = etl_competencias(event)

        # if etl_name == "locales_propios":
        #     response = etl_locales_propios(event)

        # if etl_name == "consolidar":
        #     response = etl_consolidar(event)

        return {"status": "OK","response": response}
      else:
          raise ValueError(f"Error (schema,report_name ,etl_name , buffer_search ), No deben ser vacíos: {event}")

    except Exception as e:
        # tb = traceback.format_exc()
        response['error'] = str(e)
        return {"status": "FAIL","response": response}