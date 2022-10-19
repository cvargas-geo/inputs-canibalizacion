import boto3
import json 
from botocore.exceptions import ClientError

def get_secret(secret_name, region_name):
    print("DEBUG: Begin get_secret with secret_name:%s region_name: %s" % (secret_name, region_name))
    session = boto3.session.Session()
    client = session.client(
        service_name = 'secretsmanager',
        region_name = region_name
    )

    try:
        response = client.get_secret_value(
            SecretId = secret_name
        )
    except ClientError as e:
        print("Se ha producido un error al obtener secreto '%s'" % (secret_name))
        raise e

    print("DEBUG: End get_secret")
    return json.loads(response['SecretString'])