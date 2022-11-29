try:
    import unzip_requirements
except ImportError:
    pass
import boto3
import traceback
from etls.local import etl_local
from etls.delivery import etl_delivery
from etls.captura import etl_captura
from etls.gap import etl_gap
# from utils import conf
from utils.response import response_error, response_ok
lambda_client = boto3.client("lambda")

"""
"""


def input_validation(event):
    default_params = [
        "stage" ,
        "etl_name" ,
        "input"
    ]
    for param in default_params:
        if param not in event : 
            raise ValueError(f"Se espera {param}")


def handler( event, context):
    print(f"event --->: {event}")
    # return {"status":'DEBUG'}
    try:
        input_validation(event)
        # print("pass validation")
        etl_name      = event.get('input', None).get('etl_name', None)
        response = {"status":'void'}

        if etl_name == "local":
            response = etl_local(event)
            # print("pass local")
        if etl_name == "delivery":
            response = etl_delivery(event)
            # print("pass deliv")

        if etl_name == "captura":
            response = etl_captura(event)

        if etl_name == "gap":
            response = etl_gap(event)

        print(response)
        return   response_ok(response)

    except Exception as e:
        return response_error(str(traceback.format_exc()))