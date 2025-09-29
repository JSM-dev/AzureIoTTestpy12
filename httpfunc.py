import azure.functions as func
import json
import logging
from azure.iot.hub import IoTHubRegistryManager
from datetime import datetime  # Add this import
import os

# Create blueprint for C2D messaging
bp_c2dAPI = func.Blueprint()

@bp_c2dAPI.route(route="send-c2d", methods=["POST"])
def send_c2d_message(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('C2D message API triggered')
    
    try:
        # Get IoT Hub connection string from app settings
        iot_hub_connection_string = os.environ.get('IoTHubConnectionString')
        if not iot_hub_connection_string:
            return func.HttpResponse(
                json.dumps({"error": "IoT Hub connection string not configured"}),
                status_code=500,
                mimetype="application/json"
            )
        
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
        # Send C2D message
        registry_manager = IoTHubRegistryManager(iot_hub_connection_string)
        
        # Create the message
        message = json.dumps(c2d_message)
        
        # Send cloud-to-device message
        registry_manager.send_c2d_message(device_id, message)
        
        logging.info(f"C2D message sent to device {device_id}: {message}")
        
        return func.HttpResponse(
            json.dumps({
                "success": True,
                "deviceId": device_id,
                "command": command,
                "value": value,
                "messageId": message_id,
                "message": "C2D message sent successfully"
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error sending C2D message: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": f"Failed to send C2D message: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@bp_c2dAPI.route(route="device-commands", methods=["GET"])
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
        "usage": {
            "endpoint": "/api/send-c2d",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
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