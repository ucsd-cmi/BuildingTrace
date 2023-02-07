import os
import json


password_dir = os.path.join('..', '.env', 'password.json')
aws_dir = os.path.join('.env', 'aws_config.json')
arc_dir = os.path.join('..', '.env', 'arcgis_ucsd.json')
def auth(keyfile_path):
    '''
    set-up secrets for authentication to Google
    '''

    os.environ['SHEET_KEY_PATH'] = keyfile_path
    return

def getPassword():
    '''
    set-up password for lambda requests
    '''
    creds = json.load(open(password_dir))
    os.environ['SERVICE_PASS'] = creds.get("password")
    return

def setLambdaParams():
    '''
    get params for lambda deployment
    '''
    creds = json.load(open(aws_dir))
    os.environ['LAMBDA_ImageUri'] = creds.get("ImageUri")
    os.environ['LAMBDA_Role'] = creds.get("Role")
    return

def getArcCredentials():
    '''
    set-up password for arcgis layer updates
    '''
    creds = json.load(open(arc_dir))
    os.environ['ARC_USER'] = creds.get("user_name")
    os.environ['ARC_PASS'] = creds.get("password")
    return