import boto3
import sys
from src.env_setup import setLambdaParams
import os

def deploy_lambda():
    try:
        ImageUri = os.environ['LAMBDA_ImageUri']
        Role = os.environ['LAMBDA_Role']
    except KeyError:
        # path not yet set
        setLambdaParams()
        ImageUri = os.environ['LAMBDA_ImageUri']
        Role = os.environ['LAMBDA_Role']
    client = boto3.client('lambda')
    response = client.create_function(FunctionName="traceAPI",
    Code={
        'ImageUri':ImageUri
    },
    Timeout=300,
    MemorySize=1048,
    Role=Role,
    Publish=True,
    PackageType='Image'
    )
    print(response)

if __name__ == '__main__':
    targets = sys.argv[1:]
    deploy_lambda()