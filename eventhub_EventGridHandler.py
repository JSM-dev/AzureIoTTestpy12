import azure.functions as func
import json
import logging
import os
import re
import uuid
import base64
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

bp_eventgridHandler = func.Blueprint()

# ✅ AZURE FUNCTIONS BEST PRACTICE: Simplified patterns for performance
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
    ✅ AZURE FUNCTIONS: EventGrid handler with simplified base64 decoding.
    Following Azure Functions best practices for performance and reliability.
    """
    
    correlation_id = str(uuid.uuid4())[:8]
    batch_size = len(azeventhub)
    
    logging.info(f"[{correlation_id}] Processing {batch_size} events (Simplified base64 support)")
    
    documents = []
    error_count = 0
    cloudevents_count = 0
    base64_decoded_count = 0
    direct_payload_count = 0
    
    for i, event in enumerate(azeventhub):
        event_correlation = f"{correlation_id}_{i}"
        
        try:
            # ✅ AZURE FUNCTIONS BEST PRACTICE: Single JSON parse with error handling
            raw_data = json.loads(event.get_body().decode('utf-8'))
            
            # ✅ SIMPLIFIED: Enhanced CloudEvents extraction with simplified base64 support
            extraction_result = _extract_payload_simplified(raw_data, event_correlation)
            
            if not extraction_result['success']:
                error_count += 1
                logging.warning(f"[{event_correlation}] Payload extraction failed: {extraction_result['reason']}")
                continue
            
            payload = extraction_result['payload']
            
            # Track processing statistics
            if extraction_result.get('is_cloudevents'):
                cloudevents_count += 1
            else:
                direct_payload_count += 1
                
            if extraction_result.get('base64_decoded'):
                base64_decoded_count += 1
            
            # ✅ AZURE FUNCTIONS BEST PRACTICE: Simplified validation with essential security
            doc = _create_cosmos_document_with_validation(payload, raw_data, event, event_correlation)
            if doc:
                documents.append(doc)
                
                logging.info(
                    f"[{event_correlation}] ✅ Document created successfully",
                    extra={
                        'device_id': doc.get('DeviceId'),
                        'customer_id': doc.get('CustomerId'),
                        'device_count': doc.get('DeviceCount', 0),
                        'base64_decoded': extraction_result.get('base64_decoded', False)
                    }
                )
            else:
                error_count += 1
                
        except json.JSONDecodeError as e:
            error_count += 1
            logging.error(f"[{event_correlation}] JSON decode error: {str(e)}")
        except Exception as e:
            error_count += 1
            logging.error(f"[{event_correlation}] Processing error: {str(e)}")
    
    # ✅ AZURE FUNCTIONS BEST PRACTICE: Bulk insert with error handling
    if documents:
        try:
            cosmosout.set(func.DocumentList(documents))
            logging.info(f"[{correlation_id}] ✅ Successfully queued {len(documents)} documents for Cosmos DB")
        except Exception as cosmos_error:
            logging.error(f"[{correlation_id}] ❌ Cosmos DB error: {str(cosmos_error)}")
    
    # ✅ AZURE FUNCTIONS BEST PRACTICE: Comprehensive monitoring and health checks
    logging.info(
        f"[{correlation_id}] Batch processing complete",
        extra={
            'total_events': batch_size,
            'successful_documents': len(documents),
            'error_count': error_count,
            'cloudevents_count': cloudevents_count,
            'base64_decoded_count': base64_decoded_count,
            'direct_payload_count': direct_payload_count,
            'success_rate': (len(documents) / batch_size * 100) if batch_size > 0 else 0
        }
    )


def _extract_payload_simplified(raw_data: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    """
    ✅ SIMPLIFIED: CloudEvents extraction with simplified base64 decoding.
    Following Azure Functions best practices for minimal overhead and robust error handling.
    """
    try:
        # ✅ AZURE FUNCTIONS BEST PRACTICE: Validate input parameters
        if not isinstance(raw_data, dict):
            return {
                'success': False,
                'reason': f'Raw data is not a dictionary: {type(raw_data).__name__}',
                'payload': None
            }
        
        # ✅ CLOUDEVENTS DETECTION: Check for CloudEvents v1.0 fields
        is_cloudevents = (
            'specversion' in raw_data and 
            'type' in raw_data and 
            'source' in raw_data and
            ('data' in raw_data or 'data_base64' in raw_data)
        )
        
        if is_cloudevents:
            logging.info(
                f"[{event_id}] CloudEvents v1.0 detected",
                extra={
                    'specversion': raw_data.get('specversion'),
                    'type': raw_data.get('type'),
                    'has_data': 'data' in raw_data,
                    'has_data_base64': 'data_base64' in raw_data,
                    'datacontenttype': raw_data.get('datacontenttype')
                }
            )
            
            payload = None
            base64_decoded = False
            
            # ✅ SIMPLIFIED: Handle base64-encoded data with simplified error handling
            if 'data_base64' in raw_data:
                base64_data = raw_data['data_base64']
                
                # ✅ AZURE FUNCTIONS BEST PRACTICE: Basic validation before processing
                if not isinstance(base64_data, str) or len(base64_data) == 0:
                    logging.warning(f"[{event_id}] Invalid base64 data format")
                    return {
                        'success': False,
                        'reason': 'Invalid base64 data format',
                        'payload': None
                    }
                
                # ✅ SIMPLIFIED: Decode base64 with simple error handling
                decoded_payload = _decode_base64_simple(base64_data, event_id)
                if decoded_payload is not None:
                    payload = decoded_payload
                    base64_decoded = True
                else:
                    return {
                        'success': False,
                        'reason': 'Base64 decoding failed',
                        'payload': None
                    }
            
            # ✅ FALLBACK: Handle regular data field
            elif 'data' in raw_data:
                payload = raw_data['data']
                logging.info(f"[{event_id}] Using regular data field")
            
            if payload is not None:
                # ✅ AZURE FUNCTIONS BEST PRACTICE: Preserve CloudEvents metadata
                if isinstance(payload, dict):
                    payload['_cloudevents_metadata'] = {
                        'specversion': raw_data.get('specversion'),
                        'type': raw_data.get('type'),
                        'source': raw_data.get('source'),
                        'id': raw_data.get('id'),
                        'time': raw_data.get('time'),
                        'subject': raw_data.get('subject'),
                        'datacontenttype': raw_data.get('datacontenttype')
                    }
                    payload['_is_cloudevents'] = True
                    payload['_base64_decoded'] = base64_decoded
                
                return {
                    'success': True,
                    'reason': 'cloudevents_payload_extracted',
                    'payload': payload,
                    'is_cloudevents': True,
                    'base64_decoded': base64_decoded
                }
            else:
                return {
                    'success': False,
                    'reason': 'CloudEvents detected but no valid data found',
                    'payload': None
                }
        else:
            # ✅ DIRECT PAYLOAD: Handle non-CloudEvents format
            logging.info(f"[{event_id}] Direct payload format detected")
            raw_data['_is_cloudevents'] = False
            raw_data['_base64_decoded'] = False
            
            return {
                'success': True,
                'reason': 'direct_payload_detected',
                'payload': raw_data,
                'is_cloudevents': False,
                'base64_decoded': False
            }
            
    except Exception as e:
        logging.error(f"[{event_id}] CloudEvents extraction failed: {str(e)}")
        return {
            'success': False,
            'reason': f'Extraction error: {str(e)}',
            'payload': None
        }


def _decode_base64_simple(base64_data: str, event_id: str) -> Optional[Dict[str, Any]]:
    """
    ✅ SIMPLIFIED: Simple base64 decoding with minimal error handling.
    Following Azure Functions best practices for performance and reliability.
    """
    try:
        logging.info(f"[{event_id}] Decoding base64 data (length: {len(base64_data)})")
        
        # ✅ AZURE FUNCTIONS BEST PRACTICE: Simple base64 decoding
        decoded_bytes = base64.b64decode(base64_data)
        decoded_string = decoded_bytes.decode('utf-8')
        
        logging.info(
            f"[{event_id}] Base64 decoded successfully",
            extra={
                'decoded_length': len(decoded_string),
                'decoded_preview': decoded_string[:200] + '...' if len(decoded_string) > 200 else decoded_string
            }
        )
        
        # Parse the decoded JSON
        payload = json.loads(decoded_string)
        
        logging.info(
            f"[{event_id}] JSON parsed from base64 data",
            extra={
                'payload_keys': list(payload.keys()) if isinstance(payload, dict) else 'not_dict',
                'has_device_id': 'DeviceId' in payload if isinstance(payload, dict) else False,
                'has_consumption_devices': ('Data' in payload and 
                                          'ConsumptionDevices' in payload.get('Data', {})) 
                                          if isinstance(payload, dict) else False
            }
        )
        
        return payload
        
    except Exception as decode_error:
        # ✅ SIMPLIFIED: Single exception handler for all decode errors
        logging.error(f"[{event_id}] Base64 decode failed: {str(decode_error)}")
        return None


def _create_cosmos_document_with_validation(payload: Dict[str, Any], original_event: Dict[str, Any], event: func.EventHubEvent, event_id: str) -> Optional[Dict[str, Any]]:
    """
    ✅ AZURE FUNCTIONS: Create Cosmos DB document with simplified validation.
    Following Azure Functions best practices for data validation and document structure.
    """
    try:
        # ✅ AZURE FUNCTIONS BEST PRACTICE: Input validation and sanitization
        if not isinstance(payload, dict):
            logging.warning(f"[{event_id}] Payload is not a dictionary: {type(payload).__name__}")
            return None
        
        # Extract and validate core fields with safe defaults
        device_id = str(payload.get('DeviceId', 'unknown-device'))[:50]
        customer_id = str(payload.get('CustomerId', 'default-customer'))[:50]
        location_site_id = str(payload.get('LocationSiteId', 'default-location'))[:50]
        
        # ✅ SIMPLIFIED: Basic character sanitization
        device_id = ''.join(char for char in device_id if char not in '<>"\';&')
        customer_id = ''.join(char for char in customer_id if char not in '<>"\';&')
        location_site_id = ''.join(char for char in location_site_id if char not in '<>"\';&')
        
        # ✅ AZURE FUNCTIONS BEST PRACTICE: Extract consumption devices information
        consumption_devices = payload.get('Data', {}).get('ConsumptionDevices', {})
        device_count = len(consumption_devices) if isinstance(consumption_devices, dict) else 0
        
        # Create simple device summary
        device_summary = {}
        if isinstance(consumption_devices, dict) and device_count > 0:
            device_ids = list(consumption_devices.keys())
            device_summary = {
                'device_ids': [csdid[:8] for csdid in device_ids[:5]],  # First 5 devices, truncated
                'total_readings': sum(
                    len(device.get('ConsumptionData', []))
                    for device in consumption_devices.values()
                    if isinstance(device, dict)
                )
            }
        
        # ✅ AZURE FUNCTIONS BEST PRACTICE: Optimized document structure
        document = {
            'id': str(uuid.uuid4()),
            
            # Core business fields
            'CustomerId': customer_id,
            'LocationSiteId': location_site_id,
            'DeviceId': device_id,
            
            # Device information
            'DeviceCount': device_count,
            'DeviceSummary': device_summary,
            
            # ✅ PRESERVE: CloudEvents metadata as requested
            'Source': payload.get('_cloudevents_metadata', {}).get('source'),
            'EventTime': payload.get('_cloudevents_metadata', {}).get('time'),
            'Subject': payload.get('_cloudevents_metadata', {}).get('subject'),
            'EventType': payload.get('_cloudevents_metadata', {}).get('type'),
            'DataContentType': payload.get('_cloudevents_metadata', {}).get('datacontenttype'),
            
            # ✅ PRESERVE: Complete data field as requested
            'Data': payload.get('Data', {}),
            
            # Processing metadata
            'Timestamp': datetime.now(timezone.utc).isoformat(),
            'SequenceNumber': event.sequence_number,
            'EventFormat': 'CloudEvents v1.0' if payload.get('_is_cloudevents') else 'Direct Payload',
            'Base64Decoded': payload.get('_base64_decoded', False),
            'ProcessingVersion': 'simplified-v2.0'
        }
        
        logging.info(
            f"[{event_id}] Document created successfully",
            extra={
                'device_id': device_id,
                'customer_id': customer_id,
                'location_site_id': location_site_id,
                'device_count': device_count,
                'total_readings': device_summary.get('total_readings', 0),
                'base64_decoded': payload.get('_base64_decoded', False)
            }
        )
        
        return document
        
    except Exception as e:
        logging.error(f"[{event_id}] Document creation failed: {str(e)}")
        return None