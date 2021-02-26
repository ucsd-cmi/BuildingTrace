import json
import os
import sys
from env_setup import getPassword
from trace import autoPilot
from layer_update import updateBuilding
def handler(event, context):
    try:
        service_password = os.environ['SERVICE_PASS']
    except KeyError:
        # path not yet set
        getPassword()
        service_password = os.environ['SERVICE_PASS']

    try:
        input_pass = json.loads(event["body"] or "{}").get("password","wrong")
        input_date= json.loads(event["body"] or "{}").get("date",None)
        input_mode= json.loads(event["body"] or "{}").get("mode","affected_buildings")
    except:
        input_pass = event["body"].get("password","wrong")
        input_date = event["body"].get("date",None)
        input_mode = event["body"].get("mode","affected_buildings")
    if  input_pass != service_password:
        message = "Wrong Password"
        status_code = 403
    else:
        message,status_code = "Place Holder",200
        if input_mode == "affected_buildings":
            error_message, affected_buildings = autoPilot(input_date)
        else:
            error_message, affected_buildings = updateBuilding(input_date)
        if error_message:
            message = error_message
            status_code = 400
        else:
            message = json.dumps(affected_buildings)

    # Check date format
    return {
        "statusCode": status_code,
        "headers": { "Content-Type": "application/json"},
        "body": message
    }