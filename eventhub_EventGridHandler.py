import azure.functions as func
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

bp_eventgridHandler = func.Blueprint()

# ✅ SECURITY: Enhanced regex patterns for your schema
DEVICE_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,50}$')
CUSTOMER_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,50}$')
LOCATION_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]{1,50}$')
CSDID_UUID_PATTERN = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
ILLEGAL_CHARS_PATTERN = re.compile(r'[<>"\';\\&\x00-\x1f\x7f-\x9f]')

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
def EventGrid_Handler_DeviceMap_Optimized(azeventhub: List[func.EventHubEvent], cosmosout: func.Out[func.DocumentList]):
    """
    ✅ OPTIMIZED: Azure Functions handler specifically optimized for Device Map Structure.
    Following Azure Functions best practices for your specific JSON schema.
    """
    
    correlation_id = str(uuid.uuid4())[:8]
    batch_size = len(azeventhub)
    
    logging.info(f"[{correlation_id}] Processing {batch_size} events (Device Map Structure)")
    
    documents = []
    validation_errors = 0
    security_violations = 0
    total_devices_processed = 0
    
    for i, event in enumerate(azeventhub):
        try:
            # ✅ PERFORMANCE: Single JSON parse
            raw_data = json.loads(event.get_body().decode('utf-8'))
            
            # ✅ SCHEMA-SPECIFIC: Validation optimized for your Device Map Structure
            validation_result = _validate_device_map_schema(raw_data, f"{correlation_id}_{i}")
            
            if validation_result['valid']:
                # Create document optimized for your schema
                doc = _create_device_map_document(validation_result['data'], event, correlation_id)
                if doc:
                    documents.append(doc)
                    total_devices_processed += validation_result.get('device_count', 0)
                else:
                    validation_errors += 1
            else:
                validation_errors += 1
                if validation_result.get('security_violation'):
                    security_violations += 1
                    # ✅ INTEGRATED: Security logging integrated directly
                    logging.warning(
                        f"[{correlation_id}] SECURITY VIOLATION in event {i}: {validation_result['reason']}",
                        extra={
                            'security_event': 'validation_failure',
                            'details': validation_result['reason'],
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'correlation_id': correlation_id,
                            'event_index': i
                        }
                    )
                
        except json.JSONDecodeError:
            validation_errors += 1
            logging.warning(f"[{correlation_id}] Invalid JSON in event {i}")
        except Exception as e:
            validation_errors += 1
            logging.error(f"[{correlation_id}] Event {i} processing failed: {str(e)[:100]}")
    
    # ✅ PERFORMANCE: Bulk insert to CosmosDB
    if documents:
        cosmosout.set(func.DocumentList(documents))
    
    # ✅ MONITORING: Enhanced logging for Device Map Structure
    logging.info(
        f"[{correlation_id}] Batch complete: {len(documents)} messages, {total_devices_processed} devices, {validation_errors} errors, {security_violations} security violations"
    )


def _validate_device_map_schema(data: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    """
    ✅ SCHEMA-OPTIMIZED: Validation specifically designed for your Device Map Structure.
    Following Azure Functions best practices with focus on your JSON schema.
    """
    try:
        # Handle CloudEvents format
        payload = data.get('data', data) if 'data' in data else data
        
        validation_result = {
            'valid': True,
            'data': payload.copy(),
            'security_violation': False,
            'reason': None,
            'device_count': 0
        }
        
        # ✅ SECURITY: Validate your critical identifier fields
        security_fields = [
            ('DeviceId', DEVICE_ID_PATTERN, 'device_id'),
            ('CustomerId', CUSTOMER_ID_PATTERN, 'customer_id'),
            ('LocationSiteId', LOCATION_ID_PATTERN, 'location_id')
        ]
        
        for field_name, pattern, field_type in security_fields:
            field_value = payload.get(field_name)
            
            if field_value is None:
                # Use safe default for missing fields
                validation_result['data'][field_name] = f"default-{field_type.replace('_', '-')}"
                continue
            
            # Convert to string and check length
            str_value = str(field_value)
            if len(str_value) > 50:
                validation_result['valid'] = False
                validation_result['security_violation'] = True
                validation_result['reason'] = f"{field_name} too long ({len(str_value)} chars)"
                return validation_result
            
            # ✅ INTEGRATED: Sanitization integrated directly into validation
            if ILLEGAL_CHARS_PATTERN.search(str_value):
                validation_result['valid'] = False
                validation_result['security_violation'] = True
                validation_result['reason'] = f"{field_name} contains illegal characters"
                return validation_result
            
            # ✅ SECURITY: Validate against allowlist pattern
            if not pattern.match(str_value):
                validation_result['valid'] = False
                validation_result['security_violation'] = True
                validation_result['reason'] = f"{field_name} invalid format"
                return validation_result
            
            # ✅ INTEGRATED: Apply sanitization during validation
            sanitized_value = ILLEGAL_CHARS_PATTERN.sub('', str_value)[:50]
            validation_result['data'][field_name] = sanitized_value if sanitized_value else f"sanitized_{uuid.uuid4().hex[:8]}"
        
        # ✅ SCHEMA-SPECIFIC: Validate Data.ConsumptionDevices structure
        if 'Data' not in payload:
            validation_result['data']['Data'] = {'ConsumptionDevices': {}}
        elif not isinstance(payload['Data'], dict):
            validation_result['valid'] = False
            validation_result['reason'] = "Data section must be an object"
            return validation_result
        else:
            # ✅ DEVICE MAP: Validate ConsumptionDevices structure for your schema
            consumption_devices = payload['Data'].get('ConsumptionDevices', {})
            device_validation = _validate_consumption_devices_light(consumption_devices, event_id)
            
            if device_validation['valid']:
                validation_result['device_count'] = device_validation['device_count']
                # Preserve the original structure - minimal validation only
                validation_result['data']['Data']['ConsumptionDevices'] = consumption_devices
            else:
                validation_result['valid'] = False
                validation_result['reason'] = device_validation['reason']
                return validation_result
        
        return validation_result
        
    except Exception as validation_error:
        return {
            'valid': False,
            'data': {},
            'security_violation': False,
            'reason': f"Validation error: {str(validation_error)[:100]}",
            'device_count': 0
        }


def _validate_consumption_devices_light(consumption_devices: Any, event_id: str) -> Dict[str, Any]:
    """
    ✅ DEVICE MAP: Lightweight validation for your ConsumptionDevices map structure.
    Following Azure Functions best practices for minimal overhead validation.
    """
    try:
        if not isinstance(consumption_devices, dict):
            return {
                'valid': False,
                'reason': 'ConsumptionDevices must be an object/map',
                'device_count': 0
            }
        
        device_count = len(consumption_devices)
        
        # ✅ PERFORMANCE: Limit device count for security
        if device_count > 100:
            return {
                'valid': False,
                'reason': f'Too many devices ({device_count}), maximum 100 allowed',
                'device_count': device_count
            }
        
        # ✅ SCHEMA-SPECIFIC: Quick CSDID format validation (optional but recommended)
        invalid_csdids = []
        for csdid in consumption_devices.keys():
            if not isinstance(csdid, str) or not CSDID_UUID_PATTERN.match(csdid):
                invalid_csdids.append(csdid[:20])  # Truncate for logging
        
        if invalid_csdids and len(invalid_csdids) > device_count * 0.5:  # If > 50% invalid
            return {
                'valid': False,
                'reason': f'Invalid CSDID format detected in {len(invalid_csdids)} devices',
                'device_count': device_count
            }
        
        # ✅ SUCCESS: Device map structure is valid
        return {
            'valid': True,
            'device_count': device_count,
            'invalid_csdids': len(invalid_csdids)
        }
        
    except Exception as device_error:
        return {
            'valid': False,
            'reason': f'Device validation error: {str(device_error)[:100]}',
            'device_count': 0
        }


def _create_device_map_document(validated_data: Dict[str, Any], event: func.EventHubEvent, correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    ✅ SCHEMA-OPTIMIZED: Document creation optimized for your Device Map Structure.
    Following Azure Functions best practices for your specific schema.
    """
    try:
        # ✅ PERFORMANCE: Extract device summary efficiently
        consumption_devices = validated_data.get('Data', {}).get('ConsumptionDevices', {})
        device_summary = _create_device_map_summary(consumption_devices)
        
        # ✅ SCHEMA-SPECIFIC: Create document optimized for your structure
        document = {
            'id': str(uuid.uuid4()),
            
            # ✅ SECURITY: Validated critical fields from your schema
            'CustomerId': validated_data['CustomerId'],
            'LocationSiteId': validated_data['LocationSiteId'],
            'DeviceId': validated_data['DeviceId'],
            
            # ✅ PRESERVATION: Complete payload for analytics
            'Body': validated_data,
            
            # ✅ METADATA: Query-optimized fields
            'Timestamp': datetime.now(timezone.utc).isoformat(),
            'SequenceNumber': event.sequence_number,
            'CorrelationId': correlation_id,
            
            # ✅ DEVICE MAP: Summary fields optimized for your schema
            'DeviceCount': device_summary['device_count'],
            'DeviceSummary': device_summary,
            
            # ✅ MONITORING: Processing metadata
            'ProcessingVersion': '4.1-optimized-clean',
            'SchemaType': 'device-map-structure',
            'ValidationApplied': 'security-focused-device-map'
        }
        
        return document
        
    except Exception as doc_error:
        logging.warning(f"[{correlation_id}] Document creation failed: {str(doc_error)}")
        return None


def _create_device_map_summary(consumption_devices: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ SCHEMA-SPECIFIC: Create summary optimized for your Device Map Structure.
    Following Azure Functions best practices for efficient device analysis.
    """
    try:
        if not isinstance(consumption_devices, dict):
            return {
                'device_count': 0,
                'error': 'invalid_format',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        device_count = len(consumption_devices)
        summary = {
            'device_count': device_count,
            'has_devices': device_count > 0,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if device_count > 0:
            # ✅ PERFORMANCE: Quick analysis without deep processing
            device_ids = list(consumption_devices.keys())
            summary.update({
                'first_device_id': device_ids[0][:8],  # Truncated for privacy
                'device_id_sample': [csdid[:8] for csdid in device_ids[:3]],  # First 3 devices
                'total_consumption_readings': sum(
                    len(device.get('ConsumptionData', []))
                    for device in consumption_devices.values()
                    if isinstance(device, dict) and 'ConsumptionData' in device
                )
            })
            
            # ✅ SCHEMA-SPECIFIC: Device states summary (from your schema)
            device_states = {}
            for device_data in consumption_devices.values():
                if isinstance(device_data, dict) and 'State' in device_data:
                    state = str(device_data['State'])[:50]  # Truncate long states
                    device_states[state] = device_states.get(state, 0) + 1
            
            if device_states:
                summary['device_states'] = device_states
        
        return summary
        
    except Exception as summary_error:
        return {
            'device_count': 0,
            'error': f'summary_failed: {str(summary_error)[:50]}',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }