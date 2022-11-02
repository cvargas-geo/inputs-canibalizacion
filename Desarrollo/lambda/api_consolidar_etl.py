try:
    import unzip_requirements
except ImportError:
    pass

import boto3 


from etls.consolidar import consolidar


lambda_client = boto3.client("lambda")


def handler( event, context):
    """ Se genera esta api aparte de la de reportes, pues itera un nivel menos.
        Por lo que no se considera el etl como parte del proceso, pues la idea es agruparlos en una salida generica , tipo csv 
    """
    # etl_name = event.get('etl_name', None)
    report_name = event.get('report_name', None)
    schema = event.get('schema', None)
    buffer_search = event.get('buffer_search', None)
    response = {}
    try:
        # valida que las variables no sean nulas o vacías
        if not [x for x in (schema,   report_name  , buffer) if x  == '' or x is None] :

            response = consolidar(event)
            print("api_consolidar_etl : " ,response)
            return {"status": "OK","response_consolidar": response}
        else: 
            ValueError(f"❌ Variables no deben ser nulas o vacías {event}")

    except Exception as e:
        response['error'] = str(e)
        return {"status": "FAIL","response": response}