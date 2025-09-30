import azure.functions as func
import json
import logging
from datetime import datetime, timedelta
import os
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosHttpResponseError
from typing import List, Dict, Any

# Create blueprint for Cosmos DB API
bp_cosmosAPI = func.Blueprint()

def get_cosmos_client():
    """Initialize Cosmos DB client"""
    try:
        connection_string = os.environ.get('CosmosDbConnectionString')
        if not connection_string:
            raise ValueError("CosmosDbConnectionString not configured")
        
        client = CosmosClient.from_connection_string(connection_string)
        return client
    except Exception as e:
        logging.error(f"Failed to initialize Cosmos DB client: {str(e)}")
        raise

def get_cosmos_container():
    """Get Cosmos DB container"""
    try:
        client = get_cosmos_client()
        database_name = os.environ.get('CosmosDbDatabase', 'EOKS-db-prod')
        container_name = os.environ.get('CosmosDbContainer', 'Container1')
        
        database = client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        
        return container
    except Exception as e:
        logging.error(f"Failed to get Cosmos DB container: {str(e)}")
        raise

@bp_cosmosAPI.route(route="telemetry/latest", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_latest_telemetry(req: func.HttpRequest) -> func.HttpResponse:
    """Get latest telemetry data for a device"""
    logging.info('Latest telemetry API triggered')
    
    try:
        # Get query parameters (CamelCase)
        device_id = req.params.get('DeviceId', 'TestBeckhoff')
        limit = int(req.params.get('Limit', '10'))
        
        logging.info(f"Fetching latest {limit} records for device: {device_id}")
        
        # Get Cosmos DB container
        container = get_cosmos_container()
        
        # Query for latest telemetry data
        query = """
        SELECT TOP @limit *
        FROM c 
        WHERE c.DeviceId = @deviceId 
        ORDER BY c._ts DESC
        """
        
        parameters: List[Dict[str, Any]] = [
            {"name": "@limit", "value": limit},
            {"name": "@deviceId", "value": device_id}
        ]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        logging.info(f"Found {len(items)} telemetry records")
        
        return func.HttpResponse(
            json.dumps({
                "Success": True,
                "DeviceId": device_id,
                "RecordCount": len(items),
                "Data": items,
                "Timestamp": datetime.utcnow().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "Error": "Database query failed",
                "Details": str(e),
                "StatusCode": e.status_code
            }),
            status_code=500,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error fetching telemetry data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"Error": f"Failed to fetch telemetry data: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@bp_cosmosAPI.route(route="telemetry/range", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_telemetry_range(req: func.HttpRequest) -> func.HttpResponse:
    """Get telemetry data within a time range"""
    logging.info('Telemetry range API triggered')
    
    try:
        # Get query parameters (CamelCase)
        device_id = req.params.get('DeviceId', 'TestBeckhoff')
        hours_back = int(req.params.get('HoursBack', '24'))
        limit = int(req.params.get('Limit', '100'))
        
        # Calculate time range
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        logging.info(f"Fetching telemetry for device: {device_id}, last {hours_back} hours, limit: {limit}")
        
        # Get Cosmos DB container
        container = get_cosmos_container()
        
        # Query for telemetry data in time range
        query = """
        SELECT TOP @limit *
        FROM c 
        WHERE c.DeviceId = @deviceId 
        AND c.Timestamp >= @startTime
        AND c.Timestamp <= @endTime
        ORDER BY c.Timestamp DESC
        """
        
        parameters: List[Dict[str, Any]] = [
            {"name": "@limit", "value": limit},
            {"name": "@deviceId", "value": device_id},
            {"name": "@startTime", "value": start_time.isoformat()},
            {"name": "@endTime", "value": end_time.isoformat()}
        ]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        logging.info(f"Found {len(items)} telemetry records in time range")
        
        return func.HttpResponse(
            json.dumps({
                "Success": True,
                "DeviceId": device_id,
                "TimeRange": {
                    "StartTime": start_time.isoformat(),
                    "EndTime": end_time.isoformat(),
                    "HoursBack": hours_back
                },
                "RecordCount": len(items),
                "Data": items,
                "Timestamp": datetime.utcnow().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error fetching telemetry range: {str(e)}")
        return func.HttpResponse(
            json.dumps({"Error": f"Failed to fetch telemetry range: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@bp_cosmosAPI.route(route="devices", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_devices(req: func.HttpRequest) -> func.HttpResponse:
    """Get list of all devices with recent activity"""
    logging.info('Devices list API triggered')
    
    try:
        # Get Cosmos DB container
        container = get_cosmos_container()
        
        # FIXED: Simple query to get unique device IDs (avoid GROUP BY issues)
        query = """
        SELECT DISTINCT c.DeviceId
        FROM c 
        """
        
        device_items = list(container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        
        # Get details for each device separately
        devices = []
        for device_item in device_items:
            device_id = device_item['DeviceId']
            
            # Get latest message for this device
            latest_query = """
            SELECT TOP 1 c.Timestamp, c._ts
            FROM c 
            WHERE c.DeviceId = @deviceId
            ORDER BY c._ts DESC
            """
            
            parameters: List[Dict[str, Any]] = [
                {"name": "@deviceId", "value": device_id}
            ]
            
            latest_items = list(container.query_items(
                query=latest_query,
                parameters=parameters,
                enable_cross_partition_query=True
            ))
            
            if latest_items:
                devices.append({
                    "DeviceId": device_id,
                    "LastSeen": latest_items[0].get('Timestamp'),
                    "LastActivity": latest_items[0].get('_ts')
                })
            else:
                devices.append({
                    "DeviceId": device_id,
                    "LastSeen": None,
                    "LastActivity": None
                })
        
        # Sort by last activity (most recent first)
        devices.sort(key=lambda x: x.get('LastActivity', 0), reverse=True)
        
        logging.info(f"Found {len(devices)} devices")
        
        return func.HttpResponse(
            json.dumps({
                "Success": True,
                "DeviceCount": len(devices),
                "Devices": devices,
                "Timestamp": datetime.utcnow().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error fetching devices: {str(e)}")
        return func.HttpResponse(
            json.dumps({"Error": f"Failed to fetch devices: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

@bp_cosmosAPI.route(route="telemetry/summary", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_telemetry_summary(req: func.HttpRequest) -> func.HttpResponse:
    """Get telemetry summary statistics"""
    logging.info('Telemetry summary API triggered')
    
    try:
        device_id = req.params.get('DeviceId', 'TestBeckhoff')
        hours_back = int(req.params.get('HoursBack', '24'))
        
        # Calculate time range
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        # Get Cosmos DB container
        container = get_cosmos_container()
        
        # FIXED: Simple query without aggregations (they cause issues in Cosmos DB)
        query = """
        SELECT c.Timestamp, c._ts
        FROM c 
        WHERE c.DeviceId = @deviceId 
        AND c.Timestamp >= @startTime
        AND c.Timestamp <= @endTime
        ORDER BY c.Timestamp DESC
        """
        
        parameters: List[Dict[str, Any]] = [
            {"name": "@deviceId", "value": device_id},
            {"name": "@startTime", "value": start_time.isoformat()},
            {"name": "@endTime", "value": end_time.isoformat()}
        ]
        
        items = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))
        
        # Calculate summary in Python
        if items:
            timestamps = [item.get('Timestamp') for item in items if item.get('Timestamp')]
            timestamps = [ts for ts in timestamps if ts]  # Filter out None values
            
            summary = {
                "TotalMessages": len(items),
                "FirstMessage": min(timestamps) if timestamps else None,
                "LastMessage": max(timestamps) if timestamps else None,
                "DeviceId": device_id
            }
        else:
            summary = {
                "TotalMessages": 0,
                "FirstMessage": None,
                "LastMessage": None,
                "DeviceId": device_id
            }
        
        return func.HttpResponse(
            json.dumps({
                "Success": True,
                "DeviceId": device_id,
                "TimeRange": {
                    "StartTime": start_time.isoformat(),
                    "EndTime": end_time.isoformat(),
                    "HoursBack": hours_back
                },
                "Summary": summary,
                "Timestamp": datetime.utcnow().isoformat()
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error fetching telemetry summary: {str(e)}")
        return func.HttpResponse(
            json.dumps({"Error": f"Failed to fetch telemetry summary: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )