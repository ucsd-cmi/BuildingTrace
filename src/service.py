import json
import os
import sys
import subprocess
from env_setup import getPassword
from trace import autoPilot, autoPilotManhole, traceStats
from layer_update import updateBuilding
import stat
import dateutil.parser
import datetime


def handler(event, context):
    try:
        if event['httpMethod'] == 'OPTIONS':
            print(event, "event2")
            return {
                "statusCode": 200,
                "body": "checking...",
                'headers': {
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
            }
    except:
        try:
            # Cron job workflow, separate from manual ones
            if event["detail-type"] == "Scheduled Event":
                # If it is a Cron job
                current_date_obj_utc = dateutil.parser.parse(event["time"])
                current_date_str = (
                    current_date_obj_utc-datetime.timedelta(days=1)).strftime('%-m/%-d/%y')
                current_date_obj_pst = dateutil.parser.parse(current_date_str)
                print(current_date_str)
                message, status_code = None, 200
                # if it is Monday, updates Sunday as well(only need to do this for historical mode)
                if current_date_obj_pst.weekday() == 0:
                    sun_date_str = (
                        current_date_obj_utc-datetime.timedelta(days=2)).strftime('%-m/%-d/%y')
                    error_message_sun, results = updateBuilding(
                        sun_date_str, trace_mode="historical")
                    print(results)
                    print("sunday string", sun_date_str,
                          " sunday error message ", error_message_sun)
                    if error_message_sun:
                        print(error_message_sun)
                        message = error_message_sun
                        status_code = 400
                # update normal weekdays + Saturday
                error_message_multi, results = updateBuilding(
                    current_date_str, trace_mode="multi")
                error_message_historical, results = updateBuilding(
                    current_date_str, trace_mode="historical")
                if message is None and (error_message_multi or error_message_historical):
                    message = error_message_multi or error_message_historical
                    print(message, "inside, block")
                    status_code = 400
                if message is None:
                    message = json.dumps({"message": "Cron ran smoothly"})
                return {
                    "statusCode": status_code,
                    "body": message,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                }
        except:
            pass

    try:
        service_password = os.environ['SERVICE_PASS']
    except KeyError:
        # path not yet set
        getPassword()
        service_password = os.environ['SERVICE_PASS']
    print(event)
    try:
        input_pass = json.loads(event["body"] or "{}").get("password", "wrong")
        input_date = json.loads(event["body"] or "{}").get("date", None)
        input_mode = json.loads(event["body"] or "{}").get(
            "mode", "affected_buildings")
        input_day_window = json.loads(event["body"] or "{}").get(
            "day_window", 7)
    except:
        input_pass = event["body"].get("password", "wrong")
        input_date = event["body"].get("date", None)
        input_mode = event["body"].get("mode", "affected_buildings")
        input_day_window = event["body"].get("day_window", None)
    if input_pass != service_password:
        message = "Wrong Password"
        status_code = 403
    else:
        message, status_code = "Place Holder", 200
        if input_mode == "affected_buildings":
            error_message, results = autoPilot(input_date)
        elif input_mode == "multi_update":
            error_message, results = updateBuilding(
                input_date, trace_mode="multi")
        elif input_mode == "historical":
            error_message, results = updateBuilding(
                input_date, trace_mode="historical")
        elif input_mode == "update":
            error_message, results = updateBuilding(input_date)
        elif input_mode == "drop":
            error_message, results = autoPilot(input_date, True)
        elif input_mode == "secondary_api":
            error_message, results = autoPilotManhole(input_date)
        elif input_mode == "stats":
            error_message, results = traceStats(input_date)
        else:
            error_message = "Methods not supported"
        if error_message:
            message = json.dumps({"message": error_message})
            status_code = 400
        else:
            message = json.dumps(results)

    # Check date format
    return {
        "statusCode": status_code,
        "body": message,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
    }
