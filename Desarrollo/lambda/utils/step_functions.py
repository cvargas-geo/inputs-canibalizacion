import boto3
import json 
import time
from utils.conf import REGION ,WAITING_TIME_IN_SECONDS

sfn_client = boto3.client('stepfunctions')

def iniciar_step_function(step_function_name, input_data, region_name=REGION):
    """
    Inicia un step function con los datos de entrada
    """
    response = sfn_client.start_execution(
        stateMachineArn=f"arn:aws:states:{region_name}:958531303673:stateMachine:{step_function_name}",
        input=json.dumps(input_data)
    )
    return response



def wait_step_function(execution_arn ):
    """
    Espera y valida que una step function termine exitosamente
    """
    
    ITERATIONS = 0 
    WAIT_TIME = 5
    status = "RUNNING"
    while  ITERATIONS < WAITING_TIME_IN_SECONDS:
        ITERATIONS += WAIT_TIME 
        if ITERATIONS % WAIT_TIME == 0:
            print(f"⏱️ {ITERATIONS} , Esperando maquina de estado {execution_arn} ... {status}")
            
        time.sleep(WAIT_TIME) 
        
        sf_response = sfn_client.describe_execution(executionArn=execution_arn)
        status = sf_response['status']  
        
        if status == 'SUCCEEDED':
            print(f"✔️ Esperando maquina de estado completar ... {status}")
            return True
        elif status == 'RUNNING':
            continue
        elif status == 'FAILED':
            ERROR = sf_response['QueryExecution']['Status']['StateChangeReason']
            print(f"🔥 {status} !  : {ERROR} " )
            return False

    print(f"💀 Consulta supero los {WAITING_TIME_IN_SECONDS/60} min max de ejecución : {execution_arn}") 
    return False