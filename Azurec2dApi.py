import azure.functions as func
import json
import logging
from datetime import datetime
import os
import requests

# Create blueprint for C2D messaging
bp_c2dAPI = func.Blueprint()

@bp_c2dAPI.route(route="send-c2d", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def send_c2d_message(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('C2D message API triggered')
    
    try:
        # Get IoT Hub settings from environment variables
        iot_hub_hostname = os.environ.get('IoTHubHostName')  # e.g., "ISEOS-FA-prod.azure-devices.net"
        sas_token = os.environ.get('IoTHubSasToken')  # Pre-generated SAS token
        
        if not iot_hub_hostname:
            return func.HttpResponse(
                json.dumps({"error": "IoTHubHostName not configured in app settings"}),
                status_code=500,
                headers={
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Credentials': 'false'
               }
            )
        
        if not sas_token:
            return func.HttpResponse(
                json.dumps({"error": "IoTHubSasToken not configured in app settings"}),
                status_code=500,
                headers={
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Credentials': 'false'
               }
            )
        
        logging.info(f"Using IoT Hub: {iot_hub_hostname}")
        logging.info(f"SAS token configured: {sas_token[:50]}...")  # Log first 50 chars for verification
        
        # Parse request body
        try:
            req_body = req.get_json()
            logging.info(f"Request body received: {req_body}")
        except ValueError as e:
            logging.error(f"JSON parsing error: {str(e)}")
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON in request body"}),
                status_code=400,
               headers={
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Credentials': 'false'
               }
            )
        
        # Extract parameters
        device_id = req_body.get('deviceId', 'TestBeckhoff')
        command = req_body.get('command')
        value = req_body.get('value', '')
        message_id = req_body.get('messageId', f'msg-{int(datetime.utcnow().timestamp())}')
        
        logging.info(f"Target device: {device_id}, Command: {command}, Value: {value}")
        
        if not command:
            return func.HttpResponse(
                json.dumps({"error": "Command is required"}),
                status_code=400,
                headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Credentials': 'false'
        }
            )
        
        # Build C2D message payload
        c2d_message = {
            "command": command,
            "value": value,
            "messageId": message_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # CORRECTED: IoT Hub C2D REST API endpoint with device-specific URL
        url = f"https://{iot_hub_hostname}/devices/{device_id}/messages/devicebound?api-version=2020-03-13"
        
        # Headers for C2D messages (removed iothub-to since device is now in URL)
        headers = {
            'Authorization': sas_token,
            'Content-Type': 'application/json; charset=utf-8',
            'iothub-messageid': message_id,
            'iothub-ack': 'full'
        }
        
        message_body = json.dumps(c2d_message)
        
        logging.info(f"=== C2D REQUEST DETAILS ===")
        logging.info(f"URL: {url}")
        logging.info(f"Device ID: {device_id}")
        logging.info(f"Message ID: {message_id}")
        logging.info(f"Command: {command}")
        logging.info(f"Headers: {headers}")
        logging.info(f"Message body: {message_body}")
        
        # Send the request
        response = requests.post(url, headers=headers, data=message_body, timeout=30)
        
        logging.info(f"=== RESPONSE ===")
        logging.info(f"Status: {response.status_code}")
        logging.info(f"Headers: {dict(response.headers)}")
        if response.text:
            logging.info(f"Body: {response.text}")
        
        if response.status_code == 204:
            logging.info(f"✅ C2D message sent successfully to device {device_id}")
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "deviceId": device_id,
                    "command": command,
                    "value": value,
                    "messageId": message_id,
                    "message": "C2D message sent successfully via REST API",
                    "method": "IoT Hub REST API (Device-specific endpoint)",
                    "statusCode": response.status_code,
                    "timestamp": datetime.utcnow().isoformat(),
                    "endpoint": url
                }),
                status_code=200,
                headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Credentials': 'false'
            }
                
            )
        elif response.status_code == 401:
            logging.error(f"❌ 401 Authentication failed")
            return func.HttpResponse(
                json.dumps({
                    "error": "Authentication failed - SAS token may be expired or invalid",
                    "details": response.text,
                    "statusCode": response.status_code,
                    "endpoint": url,
                    "troubleshooting": {
                        "checkSasToken": "Verify IoTHubSasToken in app settings is valid and not expired",
                        "checkPermissions": "Ensure SAS token has ServiceConnect permissions",
                        "checkDeviceId": f"Verify device '{device_id}' exists in IoT Hub"
                    }
                }),
                status_code=401,
                headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Credentials': 'false'
                }
            )
        elif response.status_code == 404:
            logging.error(f"❌ 404 Device '{device_id}' not found")
            return func.HttpResponse(
                json.dumps({
                    "error": f"Device '{device_id}' not found in IoT Hub",
                    "statusCode": response.status_code,
                    "details": response.text,
                    "endpoint": url,
                    "suggestion": f"Check if device '{device_id}' exists and is enabled in IoT Hub"
                }),
                status_code=404,
                headers={
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Credentials': 'false'
               }
            )
        else:
            logging.error(f"❌ IoT Hub API error: {response.status_code} - {response.text}")
            return func.HttpResponse(
                json.dumps({
                    "error": f"IoT Hub API error: {response.status_code}",
                    "details": response.text,
                    "statusCode": response.status_code,
                    "endpoint": url
                }),
                status_code=500,
                headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Credentials': 'false'
                }
            )
        
    except requests.exceptions.Timeout:
        logging.error("❌ Timeout connecting to IoT Hub")
        return func.HttpResponse(
            json.dumps({"error": "Timeout connecting to IoT Hub"}),
            status_code=408,
            headers={
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                    'Access-Control-Allow-Credentials': 'false'
               }
        )
    except Exception as e:
        logging.error(f"❌ Error sending C2D message: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to send C2D message: {str(e)}"}),
            status_code=500,
            headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Allow-Credentials': 'false'
                }
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
            "authentication": "Pre-generated SAS Token + Azure Function Key",
            "note": "Using pre-generated SAS token for better performance"
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
        headers={
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Access-Control-Allow-Credentials': 'false'
        }
    )