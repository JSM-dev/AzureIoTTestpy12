import azure.functions as func
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

bp_eventgridHandler = func.Blueprint()

# Simplified patterns for basic validation
DEVICE_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,50}$')
CUSTOMER_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,50}$')
LOCATION_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,50}$')

@bp_eventgridHandler.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name='GridEventHubName',
    connection="GridEventHubConStr",
    consumer_group="$Default",
    cardinality=func.Cardinality.MANY
)
@bp_eventgridHandler.cosmos_db_output(
    arg_name="cosmosout",
    database_name=os.environ.get('CosmosDbDatabase', 'EOKS-db-prod'),
    container_name=os.environ.get('CosmosDbContainer', 'Container1'),
    connection="CosmosDbConnectionString",
    create_if_not_exists=True
)
def EventGrid_Handler(azeventhub: List[func.EventHubEvent], cosmosout: func.Out[func.DocumentList]):
    """
    Simplified Azure Functions handler optimized for CloudEvents v1.0 schema.
    Handles both direct payloads and CloudEvents wrapped messages.
    """
    
    correlation_id = str(uuid.uuid4())[:8]
    batch_size = len(azeventhub)
    
    logging.info(f"[{correlation_id}] Processing {batch_size} events (CloudEvents support)")
    
    documents = []
    error_count = 0
    cloudevents_count = 0
    direct_payload_count = 0
    
    for i, event in enumerate(azeventhub):
        try:
            # Parse JSON from event
            raw_data = json.loads(event.get_body().decode('utf-8'))
            
            # ✅ FIXED: Proper CloudEvents v1.0 detection and extraction
            payload = _extract_payload_from_cloudevents(raw_data, f"{correlation_id}_{i}")
            
            if payload.get('_is_cloudevents'):
                cloudevents_count += 1
                logging.info(f"[{correlation_id}_{i}] CloudEvents v1.0 detected")
            else:
                direct_payload_count += 1
                logging.info(f"[{correlation_id}_{i}] Direct payload detected")
            
            # Create document with simplified validation
            doc = _create_cosmos_document_simple(payload, event, f"{correlation_id}_{i}")
            if doc:
                documents.append(doc)
            else:
                error_count += 1
                
        except json.JSONDecodeError as e:
            error_count += 1
            logging.warning(f"[{correlation_id}_{i}] JSON decode error: {str(e)}")
        except Exception as e:
            error_count += 1
            logging.error(f"[{correlation_id}_{i}] Processing error: {str(e)}")
    
    # Bulk insert to Cosmos DB
    if documents:
        try:
            cosmosout.set(func.DocumentList(documents))
            logging.info(f"[{correlation_id}] ✅ Successfully queued {len(documents)} documents for Cosmos DB")
        except Exception as cosmos_error:
            logging.error(f"[{correlation_id}] ❌ Cosmos DB error: {str(cosmos_error)}")
    
    # Enhanced logging with CloudEvents statistics
    logging.info(
        f"[{correlation_id}] Batch complete: {len(documents)} stored, {error_count} errors. "
        f"CloudEvents: {cloudevents_count}, Direct: {direct_payload_count}"
    )


def _extract_payload_from_cloudevents(raw_data: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    """
    Extract payload from CloudEvents v1.0 format or return direct payload.
    Handles both CloudEvents wrapped and direct payload formats.
    """
    try:
        # ✅ CLOUDEVENTS DETECTION: Check for CloudEvents v1.0 fields
        is_cloudevents = (
            'specversion' in raw_data and 
            'type' in raw_data and 
            'source' in raw_data and
            'data' in raw_data
        )
        
        if is_cloudevents:
            logging.info(f"[{event_id}] CloudEvents v1.0 detected - extracting data field")
            
            # Extract the actual payload from the 'data' field
            payload = raw_data.get('data', {})
            
            # Add CloudEvents metadata for tracking
            payload['_cloudevents_metadata'] = {
                'specversion': raw_data.get('specversion'),
                'type': raw_data.get('type'),
                'source': raw_data.get('source'),
                'id': raw_data.get('id'),
                'time': raw_data.get('time')
            }
            payload['_is_cloudevents'] = True
            
            logging.info(f"[{event_id}] Extracted payload keys: {list(payload.keys())}")
            return payload
        else:
            logging.info(f"[{event_id}] Direct payload format detected")
            raw_data['_is_cloudevents'] = False
            return raw_data
            
    except Exception as e:
        logging.warning(f"[{event_id}] CloudEvents extraction failed: {str(e)}")
        # Fallback to raw data
        raw_data['_is_cloudevents'] = False
        return raw_data


def _create_cosmos_document_simple(payload: Dict[str, Any], event: func.EventHubEvent, event_id: str) -> Optional[Dict[str, Any]]:
    """
    Create Cosmos DB document with simplified validation.
    Optimized for performance while maintaining data integrity.
    """
    try:
        # Extract device info with safe defaults
        device_id = str(payload.get('DeviceId', 'unknown-device'))[:50]
        customer_id = str(payload.get('CustomerId', 'default-customer'))[:50]
        location_site_id = str(payload.get('LocationSiteId', 'default-location'))[:50]
        
        # Basic validation - only check for obviously malicious content
        for field_name, field_value in [('DeviceId', device_id), ('CustomerId', customer_id), ('LocationSiteId', location_site_id)]:
            if any(char in field_value for char in '<>"\';&'):
                logging.warning(f"[{event_id}] Suspicious characters in {field_name}: {field_value}")
                # Sanitize by removing suspicious characters
                field_value = ''.join(char for char in field_value if char not in '<>"\';&')
                if field_name == 'DeviceId':
                    device_id = field_value
                elif field_name == 'CustomerId':
                    customer_id = field_value
                elif field_name == 'LocationSiteId':
                    location_site_id = field_value
        
        # Extract consumption devices count for summary
        consumption_devices = payload.get('Data', {}).get('ConsumptionDevices', {})
        device_count = len(consumption_devices) if isinstance(consumption_devices, dict) else 0
        
        # Create streamlined document
        document = {
            'id': str(uuid.uuid4()),
            'CustomerId': customer_id,
            'LocationSiteId': location_site_id,
            'DeviceId': device_id,
            'Body': payload,  # Store complete payload
            'Timestamp': datetime.now(timezone.utc).isoformat(),
            'SequenceNumber': event.sequence_number,
            'DeviceCount': device_count,
            'EventFormat': 'CloudEvents v1.0' if payload.get('_is_cloudevents') else 'Direct Payload',
            'ProcessingVersion': 'simplified-v1.0'
        }
        
        logging.info(f"[{event_id}] Document created - ID: {document['id']}, DeviceId: {device_id}, DeviceCount: {device_count}")
        return document
        
    except Exception as e:
        logging.error(f"[{event_id}] Document creation failed: {str(e)}")
        return None