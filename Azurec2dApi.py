import azure.functions as func
import json
import logging
from datetime import datetime, timedelta
import os
import requests
import base64
import hmac
import hashlib
import urllib.parse

# Create blueprint for C2D messaging
bp_c2dAPI = func.Blueprint()

def generate_sas_token(uri, key, policy_name, expiry_hours=1):
    """Generate SAS token for IoT Hub REST API authentication"""
    expiry = datetime.utcnow() + timedelta(hours=expiry_hours)
    expiry_timestamp = int(expiry.timestamp())
    
    # Create the string to sign
    string_to_sign = f"{uri}\n{expiry_timestamp}"
    
    # Create the signature
    key_bytes = base64.b64decode(key)
    signature = base64.b64encode(
        hmac.new(key_bytes, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
    ).decode()
    
    # Create the SAS token
    sas_token = f"SharedAccessSignature sr={uri}&sig={urllib.parse.quote(signature)}&se={expiry_timestamp}&skn={policy_name}"
    return sas_token

@bp_c2dAPI.route(route="send-c2d", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def send_c2d_message(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('C2D message API triggered')
    
    try:
        # Parse IoT Hub connection string
        connection_string = os.environ.get('IoTHubConnectionString')
        if not connection_string:
            return func.HttpResponse(
                json.dumps({"error": "IoT Hub connection string not configured"}),
                status_code=500,
                mimetype="application/json"
            )
        
        # Extract connection string components
        # Format: HostName=hubname.azure-devices.net;SharedAccessKeyName=service;SharedAccessKey=key
        parts = dict(item.split('=', 1) for item in connection_string.split(';'))
        hub_name = parts['HostName'].replace('.azure-devices.net', '')
        key_name = parts['SharedAccessKeyName']
        key_value = parts['SharedAccessKey']
        
        # Parse request body
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON in request body"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Extract parameters
        device_id = req_body.get('deviceId', 'TestBeckhoff')
        command = req_body.get('command')
        value = req_body.get('value', '')
        message_id = req_body.get('messageId', 'cmd-001')
        
        if not command:
            return func.HttpResponse(
                json.dumps({"error": "Command is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Build C2D message payload
        c2d_message = {
            "command": command,
            "value": value,
            "messageId": message_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send via IoT Hub REST API
        uri = f"{hub_name}.azure-devices.net"
        sas_token = generate_sas_token(uri, key_value, key_name)
        
        url = f"https://{uri}/messages/devicebound?api-version=2020-03-13"
        headers = {
            'Authorization': sas_token,
            'Content-Type': 'application/json',
            'iothub-to': f'/devices/{device_id}/messages/devicebound'
        }
        
        response = requests.post(url, headers=headers, data=json.dumps(c2d_message))
        
        if response.status_code == 204:
            logging.info(f"C2D message sent to device {device_id}: {json.dumps(c2d_message)}")
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "deviceId": device_id,
                    "command": command,
                    "value": value,
                    "messageId": message_id,
                    "message": "C2D message sent successfully via REST API",
                    "method": "IoT Hub REST API"
                }),
                status_code=200,
                mimetype="application/json"
            )
        else:
            logging.error(f"IoT Hub API error: {response.status_code} - {response.text}")
            return func.HttpResponse(
                json.dumps({"error": f"IoT Hub API error: {response.status_code} - {response.text}"}),
                status_code=500,
                mimetype="application/json"
            )
        
    except Exception as e:
        logging.error(f"Error sending C2D message: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to send C2D message: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@bp_c2dAPI.route(route="device-commands", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_available_commands(req: func.HttpRequest) -> func.HttpResponse:
    """Return list of available commands for the device"""
    
    available_commands = {
        "commands": [
            {
                "command": "setTemperature",
                "description": "Set temperature setpoint",
                "valueType": "number",
                "example": {"command": "setTemperature", "value": "25.5"}
            },
            {
                "command": "deviceAction", 
                "description": "Execute device action",
                "valueType": "string",
                "options": ["restart", "calibrate", "status", "reset"],
                "example": {"command": "deviceAction", "value": "calibrate"}
            },
            {
                "command": "updateInterval",
                "description": "Update telemetry interval", 
                "valueType": "string",
                "example": {"command": "updateInterval", "value": "T#30S"}
            }
        ],
        "info": {
            "method": "IoT Hub REST API",
            "authentication": "Azure Function Key",
            "note": "Using REST API for maximum compatibility"
        },
        "usage": {
            "endpoint": "/api/send-c2d?code=your-function-key",
            "method": "POST",
            "headers": {
                "Content-Type": "application/json"
            },
            "body": {
                "deviceId": "TestBeckhoff",
                "command": "setTemperature", 
                "value": "25.5",
                "messageId": "optional-message-id"
            }
        }
    }
    
    return func.HttpResponse(
        json.dumps(available_commands, indent=2),
        status_code=200,
        mimetype="application/json"
    )