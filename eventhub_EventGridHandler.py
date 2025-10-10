import azure.functions as func
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

# Create blueprint for EventGrid Handler
bp_eventgridHandler = func.Blueprint()

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
    Optimized Azure Function to process multi-schema events from EventHub and store in Cosmos DB.
    
    Supports:
    - CloudEvents v1.0 schema (auto-converts to EventGrid)
    - EventGrid schema (processes directly)
    - Raw business data (wraps in EventGrid format)
    
    Features:
    - Multi-schema event detection and processing
    - Comprehensive event inspection logging
    - Batch processing with correlation tracking
    - Input validation and sanitization
    - Error handling with retry-friendly patterns
    """
    
    # Generate correlation ID for this batch
    correlation_id = str(uuid.uuid4())
    batch_size = len(azeventhub)
    
    logging.info(f"[{correlation_id}] EventGrid Handler triggered with {batch_size} events")
    
    # Initialize counters for monitoring
    processed_count = 0
    error_count = 0
    documents_to_insert = []
    
    try:
        for event_index, event in enumerate(azeventhub):
            event_correlation_id = f"{correlation_id}_event_{event_index}"
            
            try:
                # Comprehensive event inspection for debugging
                _log_event_inspection(event, event_correlation_id, event_index)
                
                # Extract event metadata
                event_metadata = {
                    'sequence_number': event.sequence_number,
                    'offset': event.offset,
                    'enqueued_time': event.enqueued_time.isoformat() if event.enqueued_time else None,
                    'partition_key': event.partition_key,
                    'correlation_id': event_correlation_id
                }
                
                # Parse and validate event with multi-schema support
                event_data = _parse_and_validate_event(event.get_body().decode('utf-8'), event_correlation_id)
                
                if event_data is None:
                    error_count += 1
                    continue
                
                # Transform data for Cosmos DB
                cosmos_document = _transform_to_cosmos_document(event_data, event_metadata, event_correlation_id)
                
                if cosmos_document:
                    documents_to_insert.append(cosmos_document)
                    processed_count += 1
                    logging.debug(f"[{event_correlation_id}] Event processed successfully")
                
            except Exception as event_error:
                error_count += 1
                logging.error(
                    f"[{event_correlation_id}] Error processing event {event.sequence_number}: {str(event_error)}",
                    extra={
                        'correlation_id': event_correlation_id,
                        'sequence_number': event.sequence_number,
                        'error_type': type(event_error).__name__,
                        'event_index': event_index
                    }
                )
                continue
        
        # Bulk insert to Cosmos DB
        if documents_to_insert:
            cosmosout.set(func.DocumentList(documents_to_insert))
            logging.info(f"[{correlation_id}] Successfully inserted {len(documents_to_insert)} documents to Cosmos DB")
        
        # Log batch processing summary
        logging.info(
            f"[{correlation_id}] Batch processing complete. "
            f"Processed: {processed_count}, Errors: {error_count}, Total: {batch_size}",
            extra={
                'correlation_id': correlation_id,
                'processed_count': processed_count,
                'error_count': error_count,
                'batch_size': batch_size,
                'success_rate': (processed_count / batch_size * 100) if batch_size > 0 else 0
            }
        )
        
    except Exception as batch_error:
        logging.error(
            f"[{correlation_id}] Critical error in batch processing: {str(batch_error)}",
            extra={'correlation_id': correlation_id, 'error_type': type(batch_error).__name__, 'batch_size': batch_size}
        )
        raise


def _parse_and_validate_event(event_body: str, correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    Parse and validate events with multi-schema support (CloudEvents v1.0, EventGrid, Raw Data).
    Optimized single function replacing multiple parsing implementations.
    """
    try:
        event_data = json.loads(event_body)
        
        if not isinstance(event_data, dict):
            logging.warning(f"[{correlation_id}] Event data is not a dictionary: {type(event_data)}")
            return None
        
        # Detect schema format and process accordingly
        schema_format = _detect_schema_format(event_data)
        
        if schema_format == 'cloudevents_v1.0':
            logging.info(f"[{correlation_id}] Processing CloudEvents v1.0 schema")
            return _process_cloudevents(event_data, correlation_id)
        elif schema_format == 'eventgrid':
            logging.info(f"[{correlation_id}] Processing EventGrid schema")
            return _process_eventgrid(event_data, correlation_id)
        else:
            logging.info(f"[{correlation_id}] Processing raw business data - creating EventGrid wrapper")
            return _process_raw_data(event_data, correlation_id)
        
    except json.JSONDecodeError as json_error:
        logging.error(
            f"[{correlation_id}] JSON decode error: {str(json_error)}",
            extra={'event_body_preview': event_body[:200] if event_body else 'Empty', 'error_type': 'json_decode_error'}
        )
        return None
    except Exception as validation_error:
        logging.error(
            f"[{correlation_id}] Event validation error: {str(validation_error)}",
            extra={'error_type': type(validation_error).__name__}
        )
        return None


def _detect_schema_format(data: Dict[str, Any]) -> str:
    """
    Optimized schema detection for CloudEvents v1.0, EventGrid, and raw business data.
    """
    # CloudEvents v1.0 detection
    if ('type' in data and 'source' in data and 'id' in data and 
        data.get('specversion', '').startswith('1.0')):
        return 'cloudevents_v1.0'
    
    # EventGrid schema detection
    eventgrid_fields = ['eventType', 'subject', 'data', 'eventTime', 'id']
    if sum(1 for field in eventgrid_fields if field in data) >= 4:
        return 'eventgrid'
    
    # Default to raw business data
    return 'raw_business_data'


def _process_cloudevents(event_data: Dict[str, Any], correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    Process CloudEvents v1.0 schema and convert to EventGrid format.
    FIXED: Properly extract and preserve MQTT payload data.
    """
    try:
        # Validate required CloudEvents fields
        required_fields = ['type', 'source', 'id', 'specversion']
        missing_fields = [field for field in required_fields if field not in event_data]
        
        if missing_fields:
            logging.warning(
                f"[{correlation_id}] Missing CloudEvents fields: {missing_fields}",
                extra={'missing_fields': missing_fields, 'validation_type': 'cloudevents_validation'}
            )
            return None
        
        # ✅ FIXED: Properly extract MQTT business data from CloudEvents
        business_data = event_data.get('data', {})
        
        # ✅ ADD: Debug logging to see actual CloudEvents structure
        logging.info(
            f"[{correlation_id}] CloudEvents data extraction debug",
            extra={
                'cloudevents_keys': list(event_data.keys()),
                'data_field_type': type(business_data).__name__,
                'data_field_keys': list(business_data.keys()) if isinstance(business_data, dict) else 'not_dict',
                'raw_data_preview': str(business_data)[:300] if business_data else 'empty'
            }
        )
        
        # ✅ FIXED: Validate business data WITHOUT overriding existing values
        validated_business_data = _validate_business_data_preserve_original(business_data, correlation_id)
        
        # Convert to EventGrid format
        eventgrid_event = {
            'eventType': event_data.get('type', 'Microsoft.EventGrid.CloudEvent'),
            'subject': event_data.get('source', '/unknown'),
            'data': validated_business_data,  # ✅ FIXED: Use properly validated data
            'eventTime': event_data.get('time', datetime.now(timezone.utc).isoformat()),
            'id': event_data.get('id', str(uuid.uuid4())),
            'dataVersion': event_data.get('datacontenttype', '1.0'),
            'metadataVersion': '1'
        }
        
        _sanitize_event_data(eventgrid_event)
        
        logging.info(
            f"[{correlation_id}] CloudEvents converted to EventGrid format",
            extra={
                'original_type': event_data.get('type'),
                'converted_eventType': eventgrid_event['eventType'],
                'preserved_device_id': validated_business_data.get('DeviceId'),
                'preserved_customer_id': validated_business_data.get('CustomerId'),
                'preserved_location_id': validated_business_data.get('LocationSiteId'),
                'validation_type': 'cloudevents_converted'
            }
        )
        
        return eventgrid_event
        
    except Exception as conversion_error:
        logging.error(
            f"[{correlation_id}] CloudEvents conversion failed: {str(conversion_error)}",
            extra={'error_type': type(conversion_error).__name__}
        )
        return None


def _process_eventgrid(event_data: Dict[str, Any], correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    Process EventGrid schema events.
    """
    try:
        # Validate required EventGrid fields
        required_fields = ['eventType', 'subject', 'data', 'eventTime', 'id']
        missing_fields = [field for field in required_fields if field not in event_data]
        
        if missing_fields:
            logging.warning(
                f"[{correlation_id}] Missing EventGrid fields: {missing_fields}",
                extra={'missing_fields': missing_fields, 'validation_type': 'eventgrid_validation'}
            )
            return None
        
        # Validate business data
        business_data = event_data.get('data', {})
        if isinstance(business_data, dict):
            validated_business_data = _validate_business_data(business_data, correlation_id)
            event_data['data'] = validated_business_data
        
        _sanitize_event_data(event_data)
        
        logging.debug(
            f"[{correlation_id}] EventGrid event validated successfully",
            extra={'event_type': event_data['eventType'], 'validation_type': 'eventgrid_success'}
        )
        
        return event_data
        
    except Exception as validation_error:
        logging.error(
            f"[{correlation_id}] EventGrid validation failed: {str(validation_error)}",
            extra={'error_type': type(validation_error).__name__}
        )
        return None


def _process_raw_data(business_data: Dict[str, Any], correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    Process raw business data and wrap in EventGrid format.
    """
    try:
        validated_business_data = _validate_business_data(business_data, correlation_id)
        
        # Create EventGrid wrapper
        eventgrid_event = {
            'eventType': f"Custom.Business.{validated_business_data.get('EventType', 'Generic')}",
            'subject': f"/devices/{validated_business_data.get('DeviceId', 'unknown')}",
            'data': validated_business_data,
            'eventTime': datetime.now(timezone.utc).isoformat(),
            'id': validated_business_data.get('id', str(uuid.uuid4())),
            'dataVersion': '1.0',
            'metadataVersion': '1'
        }
        
        _sanitize_event_data(eventgrid_event)
        
        logging.info(
            f"[{correlation_id}] Raw data wrapped in EventGrid format",
            extra={
                'created_eventType': eventgrid_event['eventType'],
                'device_id': validated_business_data.get('DeviceId'),
                'validation_type': 'raw_data_wrapped'
            }
        )
        
        return eventgrid_event
        
    except Exception as wrap_error:
        logging.error(
            f"[{correlation_id}] Raw data wrapping failed: {str(wrap_error)}",
            extra={'error_type': type(wrap_error).__name__}
        )
        return None


def _validate_business_data_preserve_original(business_data: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    ✅ ENHANCED: Business data validation for Device Map Structure.
    Handles the new ConsumptionDevices map with CSDID keys.
    Following Azure Functions best practices for multi-device processing.
    """
    # Start with original data
    validated_data = business_data.copy() if isinstance(business_data, dict) else {}
    
    # Apply defaults for missing top-level fields only
    field_defaults = {
        'id': str(uuid.uuid4()),
        'SequenceNumber': 0,
        'CustomerId': 'default-customer',
        'LocationSiteId': 'default-location',
        'DeviceId': 'default-device',
        'EventType': 'multi-device-consumption',
        'Timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Preserve original values, only use defaults when truly missing
    applied_defaults = []
    preserved_values = []
    
    for field, default_value in field_defaults.items():
        if field not in validated_data or validated_data[field] is None or validated_data[field] == "":
            validated_data[field] = default_value
            applied_defaults.append(field)
        else:
            preserved_values.append(field)
    
    # ✅ NEW: Process Device Map Structure in Data.ConsumptionDevices
    if 'Data' in business_data and isinstance(business_data['Data'], dict):
        data_section = business_data['Data']
        validated_data_section = _validate_data_section_with_device_map(data_section, correlation_id)
        validated_data['Data'] = validated_data_section
    else:
        logging.warning(f"[{correlation_id}] No Data section found or Data is not a dictionary")
        validated_data['Data'] = {}
    
    # Log validation results
    logging.info(
        f"[{correlation_id}] Business data validation results for Device Map Structure",
        extra={
            'preserved_fields': preserved_values,
            'defaulted_fields': applied_defaults,
            'original_keys': list(business_data.keys()) if isinstance(business_data, dict) else [],
            'final_device_id': validated_data.get('DeviceId'),
            'final_customer_id': validated_data.get('CustomerId'),
            'final_location_id': validated_data.get('LocationSiteId'),
            'has_data_section': 'Data' in validated_data,
            'has_consumption_devices': 'ConsumptionDevices' in validated_data.get('Data', {}),
            'device_count': len(validated_data.get('Data', {}).get('ConsumptionDevices', {})),
            'validation_type': 'device_map_structure'
        }
    )
    
    # Sanitize string fields while preserving values
    string_fields = ['CustomerId', 'LocationSiteId', 'DeviceId', 'EventType']
    for field in string_fields:
        if field in validated_data and isinstance(validated_data[field], str):
            original_value = validated_data[field]
            validated_data[field] = _sanitize_field(validated_data[field])
            if original_value != validated_data[field]:
                logging.debug(f"[{correlation_id}] Sanitized {field}: '{original_value}' -> '{validated_data[field]}'")
    
    return _sanitize_data_recursive(validated_data)


def _validate_data_section_with_device_map(data_section: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    ✅ NEW: Validate the Data section containing ConsumptionDevices map.
    Following Azure Functions best practices for device map processing.
    """
    try:
        validated_data_section = data_section.copy()
        
        # ✅ CORE: Process ConsumptionDevices map structure
        if 'ConsumptionDevices' in data_section and isinstance(data_section['ConsumptionDevices'], dict):
            consumption_devices = data_section['ConsumptionDevices']
            
            logging.info(
                f"[{correlation_id}] Processing ConsumptionDevices map",
                extra={
                    'device_map_keys': list(consumption_devices.keys()),
                    'device_count': len(consumption_devices),
                    'processing_type': 'device_map_validation'
                }
            )
            
            validated_devices = _validate_consumption_devices_map(consumption_devices, correlation_id)
            validated_data_section['ConsumptionDevices'] = validated_devices
            
        else:
            logging.warning(f"[{correlation_id}] No ConsumptionDevices map found or not a dictionary")
            validated_data_section['ConsumptionDevices'] = {}
        
        # Preserve any other fields in the Data section
        for field_name, field_value in data_section.items():
            if field_name != 'ConsumptionDevices':
                validated_data_section[field_name] = field_value
                logging.debug(f"[{correlation_id}] Preserved additional Data field: {field_name}")
        
        return validated_data_section
        
    except Exception as data_validation_error:
        logging.error(
            f"[{correlation_id}] Data section validation failed: {str(data_validation_error)}",
            extra={'error_type': type(data_validation_error).__name__}
        )
        return {'ConsumptionDevices': {}}


def _validate_consumption_devices_map(devices_map: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    ✅ NEW: Validate ConsumptionDevices map with CSDID keys.
    Following Azure Functions best practices for error isolation and performance.
    """
    try:
        validated_devices = {}
        processing_stats = {
            'total_devices': len(devices_map),
            'processed_devices': 0,
            'failed_devices': 0,
            'device_processing_results': []
        }
        
        # ✅ PERFORMANCE: Limit device count to prevent memory issues
        max_devices = 100  # Configurable limit
        if len(devices_map) > max_devices:
            logging.warning(
                f"[{correlation_id}] Device map too large ({len(devices_map)} devices), processing first {max_devices}",
                extra={'original_count': len(devices_map), 'processed_count': max_devices}
            )
            # Process first N devices (could be changed to most recent, etc.)
            devices_to_process = dict(list(devices_map.items())[:max_devices])
        else:
            devices_to_process = devices_map
        
        # ✅ CORE: Process each device in the map
        for csdid, device_data in devices_to_process.items():
            device_correlation_id = f"{correlation_id}_device_{csdid[:8]}"  # Short CSDID for logging
            
            try:
                # Validate CSDID format
                if not _is_valid_csdid(csdid):
                    logging.warning(f"[{device_correlation_id}] Invalid CSDID format: {csdid}")
                    processing_stats['failed_devices'] += 1
                    continue
                
                # Validate individual device
                validated_device = _validate_single_consumption_device(device_data, device_correlation_id, csdid)
                
                if validated_device is not None:
                    validated_devices[csdid] = validated_device
                    processing_stats['processed_devices'] += 1
                    processing_stats['device_processing_results'].append({
                        'csdid': csdid,
                        'status': 'success',
                        'name': validated_device.get('Name', 'unnamed'),
                        'consumption_data_count': len(validated_device.get('ConsumptionData', []))
                    })
                    
                    logging.debug(f"[{device_correlation_id}] Device processed successfully")
                else:
                    processing_stats['failed_devices'] += 1
                    processing_stats['device_processing_results'].append({
                        'csdid': csdid,
                        'status': 'failed',
                        'reason': 'validation_failed'
                    })
                    logging.warning(f"[{device_correlation_id}] Device validation failed")
                
            except Exception as device_error:
                processing_stats['failed_devices'] += 1
                processing_stats['device_processing_results'].append({
                    'csdid': csdid,
                    'status': 'error',
                    'error': str(device_error)
                })
                logging.error(
                    f"[{device_correlation_id}] Device processing error: {str(device_error)}",
                    extra={'csdid': csdid, 'error_type': type(device_error).__name__}
                )
                continue
        
        # ✅ MONITORING: Log comprehensive device map processing results
        logging.info(
            f"[{correlation_id}] ConsumptionDevices map processing complete",
            extra={
                'processing_stats': processing_stats,
                'success_rate': (processing_stats['processed_devices'] / processing_stats['total_devices'] * 100) if processing_stats['total_devices'] > 0 else 0,
                'final_device_count': len(validated_devices),
                'validated_device_ids': list(validated_devices.keys()),
                'processing_type': 'device_map_complete'
            }
        )
        
        return validated_devices
        
    except Exception as map_validation_error:
        logging.error(
            f"[{correlation_id}] ConsumptionDevices map validation failed: {str(map_validation_error)}",
            extra={'error_type': type(map_validation_error).__name__}
        )
        return {}


def _validate_single_consumption_device(device_data: Any, correlation_id: str, csdid: str) -> Optional[Dict[str, Any]]:
    """
    ✅ NEW: Validate individual consumption device with flexible field support.
    Following Azure Functions best practices for data preservation and validation.
    """
    try:
        if not isinstance(device_data, dict):
            logging.warning(f"[{correlation_id}] Device data is not a dictionary: {type(device_data)}")
            return None
        
        validated_device = device_data.copy()
        processing_info = {
            'original_fields': list(device_data.keys()),
            'preserved_fields': [],
            'processed_consumption_data': False,
            'consumption_data_count': 0
        }
        
        # ✅ FLEXIBLE: Preserve all device fields
        for field_name, field_value in device_data.items():
            if field_name == 'ConsumptionData':
                # Special handling for ConsumptionData arrays
                validated_consumption_data = _validate_consumption_data_array_flexible(field_value, correlation_id)
                if validated_consumption_data is not None:
                    validated_device['ConsumptionData'] = validated_consumption_data
                    processing_info['processed_consumption_data'] = True
                    processing_info['consumption_data_count'] = len(validated_consumption_data)
                else:
                    validated_device['ConsumptionData'] = []
                    logging.warning(f"[{correlation_id}] ConsumptionData validation failed, using empty array")
            
            elif field_name == 'Name':
                # Sanitize device name
                if isinstance(field_value, str) and field_value.strip():
                    validated_device['Name'] = _sanitize_field(field_value)
                    processing_info['preserved_fields'].append('Name')
                else:
                    validated_device['Name'] = f"Device_{csdid[:8]}"  # Fallback name
                    logging.debug(f"[{correlation_id}] Applied fallback name for device")
            
            elif field_name == 'State':
                # Preserve state information as-is (it's device-specific)
                validated_device['State'] = str(field_value) if field_value is not None else ""
                processing_info['preserved_fields'].append('State')
            
            else:
                # Preserve all other fields dynamically
                validated_device[field_name] = field_value
                processing_info['preserved_fields'].append(field_name)
        
        # ✅ DEFAULTS: Add missing critical fields with sensible defaults
        if 'Name' not in validated_device:
            validated_device['Name'] = f"Device_{csdid[:8]}"
        
        if 'ConsumptionData' not in validated_device:
            validated_device['ConsumptionData'] = []
        
        if 'State' not in validated_device:
            validated_device['State'] = "Unknown"
        
        # ✅ MONITORING: Log device processing results
        logging.debug(
            f"[{correlation_id}] Device validation complete",
            extra={
                'csdid': csdid,
                'device_name': validated_device.get('Name'),
                'processing_info': processing_info,
                'final_field_count': len(validated_device),
                'validation_type': 'single_device_complete'
            }
        )
        
        return validated_device
        
    except Exception as device_validation_error:
        logging.error(
            f"[{correlation_id}] Single device validation failed: {str(device_validation_error)}",
            extra={'csdid': csdid, 'error_type': type(device_validation_error).__name__}
        )
        return None


def _validate_consumption_data_array_flexible(consumption_data: Any, correlation_id: str) -> Optional[List[Dict[str, Any]]]:
    """
    ✅ ENHANCED: Flexible ConsumptionData validation supporting any dynamic fields.
    Reusing the flexible validation logic from previous implementation.
    Following Azure Functions best practices for schema-agnostic processing.
    """
    try:
        # Handle different input formats
        if isinstance(consumption_data, dict):
            # Single consumption reading - convert to array format
            logging.debug(f"[{correlation_id}] Converting single ConsumptionData object to array format")
            consumption_array = [consumption_data]
        elif isinstance(consumption_data, list):
            # Array of consumption readings
            logging.debug(f"[{correlation_id}] Processing ConsumptionData array with {len(consumption_data)} readings")
            consumption_array = consumption_data
        else:
            logging.warning(f"[{correlation_id}] Invalid ConsumptionData format: {type(consumption_data)}")
            return None
        
        # ✅ PERFORMANCE: Limit array size to prevent memory issues
        max_readings = 1000  # Configurable limit
        if len(consumption_array) > max_readings:
            logging.warning(
                f"[{correlation_id}] ConsumptionData array too large ({len(consumption_array)} readings), truncating to {max_readings}"
            )
            consumption_array = consumption_array[:max_readings]
        
        # Validate each reading with flexible schema
        validated_readings = []
        discovered_fields = set()
        
        for index, reading in enumerate(consumption_array):
            validated_reading = _validate_single_consumption_reading_flexible(
                reading, correlation_id, index, discovered_fields
            )
            if validated_reading is not None:
                validated_readings.append(validated_reading)
        
        if not validated_readings:
            logging.warning(f"[{correlation_id}] No valid consumption readings found")
            return []
        
        logging.debug(
            f"[{correlation_id}] Successfully validated {len(validated_readings)} consumption readings",
            extra={
                'total_readings': len(validated_readings),
                'discovered_fields': sorted(list(discovered_fields)),
                'schema_flexibility': 'dynamic_fields_supported'
            }
        )
        
        return validated_readings
        
    except Exception as validation_error:
        logging.error(
            f"[{correlation_id}] ConsumptionData array validation failed: {str(validation_error)}",
            extra={'error_type': type(validation_error).__name__}
        )
        return []


def _validate_single_consumption_reading_flexible(
    reading: Any, 
    correlation_id: str, 
    index: int, 
    discovered_fields: set
) -> Optional[Dict[str, Any]]:
    """
    ✅ ENHANCED: Flexible validation for individual consumption readings supporting any fields.
    Following Azure Functions best practices for dynamic schema handling.
    """
    try:
        if not isinstance(reading, dict):
            logging.warning(f"[{correlation_id}] Consumption reading {index} is not a dictionary: {type(reading)}")
            return None
        
        validated_reading = {}
        
        # ✅ FLEXIBLE: Process all fields dynamically
        for field_name, field_value in reading.items():
            discovered_fields.add(field_name)
            
            # ✅ SMART: Handle different data types intelligently
            if field_name.lower() in ['timestamp', 'time', 'datetime', 'created', 'updated']:
                # Handle timestamp fields flexibly
                validated_timestamp = _validate_and_normalize_timestamp(field_value, correlation_id, index)
                validated_reading[field_name] = validated_timestamp
                
            elif isinstance(field_value, (int, float)):
                # Numeric fields - preserve as-is with safe range checking
                if _is_reasonable_numeric_value(field_value):
                    validated_reading[field_name] = field_value
                else:
                    logging.warning(f"[{correlation_id}] Extreme numeric value for {field_name}: {field_value} in reading {index}")
                    validated_reading[field_name] = field_value  # Preserve anyway, but log warning
                
            elif isinstance(field_value, str):
                # String fields - sanitize but preserve
                if field_value.strip():
                    validated_reading[field_name] = _sanitize_field(field_value)
                else:
                    validated_reading[field_name] = field_value
                
            elif isinstance(field_value, bool):
                # Boolean fields - preserve as-is
                validated_reading[field_name] = field_value
                
            elif isinstance(field_value, (dict, list)):
                # Complex structures - preserve as-is after recursive sanitization
                validated_reading[field_name] = _sanitize_data_recursive(field_value)
                
            elif field_value is None:
                # Null values - preserve as-is
                validated_reading[field_name] = field_value
                
            else:
                # Unknown types - convert to string representation
                try:
                    validated_reading[field_name] = _sanitize_field(str(field_value))
                except Exception:
                    logging.warning(f"[{correlation_id}] Failed to convert field {field_name} in reading {index}")
                    continue
        
        # ✅ SMART: Add timestamp if not present in any form
        timestamp_fields = [field for field in validated_reading.keys() 
                          if field.lower() in ['timestamp', 'time', 'datetime', 'created', 'updated']]
        
        if not timestamp_fields:
            validated_reading['Timestamp'] = datetime.now(timezone.utc).isoformat()
        
        return validated_reading
        
    except Exception as reading_error:
        logging.error(
            f"[{correlation_id}] Flexible consumption reading {index} validation failed: {str(reading_error)}",
            extra={'error_type': type(reading_error).__name__}
        )
        return None


def _is_valid_csdid(csdid: str) -> bool:
    """
    ✅ NEW: Validate CSDID format (assuming UUID format based on your example).
    Following Azure Functions best practices for input validation.
    """
    try:
        if not isinstance(csdid, str) or len(csdid.strip()) == 0:
            return False
        
        # Check if it looks like a UUID (basic format check)
        import re
        uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        return bool(re.match(uuid_pattern, csdid.strip()))
        
    except Exception:
        return False


def _validate_and_normalize_timestamp(timestamp_value: Any, correlation_id: str, index: int) -> str:
    """
    ✅ REUSED: Validate and normalize timestamp to ISO 8601 format with timezone.
    Following Azure Functions best practices for data consistency.
    """
    try:
        if isinstance(timestamp_value, str):
            try:
                # Parse and convert to UTC timezone
                if timestamp_value.endswith('Z'):
                    parsed_dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
                elif '+' in timestamp_value or timestamp_value.endswith('K'):
                    clean_timestamp = timestamp_value.replace('K', 'Z')
                    parsed_dt = datetime.fromisoformat(clean_timestamp.replace('Z', '+00:00'))
                else:
                    parsed_dt = datetime.fromisoformat(timestamp_value)
                    if parsed_dt.tzinfo is None:
                        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                
                utc_dt = parsed_dt.astimezone(timezone.utc)
                return utc_dt.isoformat()
                
            except (ValueError, TypeError):
                return datetime.now(timezone.utc).isoformat()
        
        elif isinstance(timestamp_value, (int, float)):
            try:
                dt = datetime.fromtimestamp(timestamp_value, tz=timezone.utc)
                return dt.isoformat()
            except (ValueError, OSError):
                return datetime.now(timezone.utc).isoformat()
        
        else:
            return datetime.now(timezone.utc).isoformat()
    
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _is_reasonable_numeric_value(value: float) -> bool:
    """
    ✅ REUSED: Check if numeric value is within reasonable bounds for telemetry data.
    Following Azure Functions best practices for data validation.
    """
    try:
        if not isinstance(value, (int, float)) or value != value:  # NaN check
            return False
        
        if value == float('inf') or value == float('-inf'):
            return False
        
        min_bound = -1e10  # -10 billion
        max_bound = 1e10   # 10 billion
        
        return min_bound <= value <= max_bound
        
    except Exception:
        return False


def _transform_to_cosmos_document(event_data: Dict[str, Any], event_metadata: Dict[str, Any], correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    ✅ ENHANCED: Cosmos document transformation optimized for Device Map Structure.
    Following Azure Functions best practices for multi-device document creation.
    """
    try:
        data_payload = event_data.get('data', {})
        
        # ✅ ENHANCED: Generate device summary for Device Map Structure
        device_summary = _generate_device_summary(data_payload, correlation_id)
        
        cosmos_document = {
            # Standard Cosmos DB fields
            'id': str(uuid.uuid4()),
            
            # Required business fields
            'CustomerId': data_payload.get('CustomerId', 'default-customer'),
            'LocationSiteId': data_payload.get('LocationSiteId', 'default-location'),
            'DeviceId': data_payload.get('DeviceId', 'default-device'),
            
            # EventGrid fields
            'EventType': event_data.get('eventType'),
            'Subject': event_data.get('subject'),
            'EventTime': event_data.get('eventTime'),
            'DataVersion': event_data.get('dataVersion'),
            'MetadataVersion': event_data.get('metadataVersion'),
            
            # ✅ ENHANCED: Complete payload with device map structure
            'Body': data_payload,
            'Timestamp': datetime.now(timezone.utc).isoformat(),
            'CorrelationId': correlation_id,
            'Source': 'EventGrid',
            'FunctionName': 'EventGrid_Handler',
            'SequenceNumber': event_metadata.get('sequence_number'),
            'EventHub': {
                'offset': event_metadata.get('offset'),
                'enqueuedTime': event_metadata.get('enqueued_time'),
                'partitionKey': event_metadata.get('partition_key')
            },
            'ProcessingVersion': '2.0',  # Updated version for Device Map Structure
            'Environment': os.getenv('AZURE_FUNCTIONS_ENVIRONMENT', 'Development'),
            
            # ✅ NEW: Device Map summary for query optimization
            'DeviceMapSummary': device_summary
        }
        
        logging.debug(
            f"[{correlation_id}] Document transformed for Device Map Structure",
            extra={
                'document_id': cosmos_document['id'],
                'customer_id': cosmos_document['CustomerId'],
                'device_id': cosmos_document['DeviceId'],
                'device_count': device_summary.get('total_devices', 0),
                'total_consumption_readings': device_summary.get('total_consumption_readings', 0)
            }
        )
        
        return cosmos_document
        
    except Exception as transform_error:
        logging.error(
            f"[{correlation_id}] Document transformation failed: {str(transform_error)}",
            extra={'error_type': type(transform_error).__name__}
        )
        return None


def _generate_device_summary(data_payload: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    ✅ NEW: Generate summary statistics for Device Map Structure for query optimization.
    Following Azure Functions best practices for analytics support.
    """
    try:
        summary = {
            'total_devices': 0,
            'device_list': [],
            'total_consumption_readings': 0,
            'earliest_timestamp': None,
            'latest_timestamp': None,
            'device_states': {},
            'processing_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        consumption_devices = data_payload.get('Data', {}).get('ConsumptionDevices', {})
        
        if not isinstance(consumption_devices, dict):
            return summary
        
        summary['total_devices'] = len(consumption_devices)
        
        all_timestamps = []
        
        for csdid, device_data in consumption_devices.items():
            if not isinstance(device_data, dict):
                continue
            
            # Device info
            device_info = {
                'csdid': csdid,
                'name': device_data.get('Name', 'unnamed'),
                'state': device_data.get('State', 'unknown'),
                'consumption_readings': 0
            }
            
            # Count consumption readings and collect timestamps
            consumption_data = device_data.get('ConsumptionData', [])
            if isinstance(consumption_data, list):
                device_info['consumption_readings'] = len(consumption_data)
                summary['total_consumption_readings'] += len(consumption_data)
                
                # Collect timestamps
                for reading in consumption_data:
                    if isinstance(reading, dict):
                        for field_name, field_value in reading.items():
                            if field_name.lower() in ['timestamp', 'time', 'datetime'] and isinstance(field_value, str):
                                all_timestamps.append(field_value)
            
            summary['device_list'].append(device_info)
            
            # Track device states
            state = device_data.get('State', 'unknown')
            if state not in summary['device_states']:
                summary['device_states'][state] = 0
            summary['device_states'][state] += 1
        
        # Determine timestamp range
        if all_timestamps:
            try:
                parsed_timestamps = []
                for ts in all_timestamps:
                    try:
                        if ts.endswith('Z'):
                            parsed_ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                        else:
                            parsed_ts = datetime.fromisoformat(ts)
                        parsed_timestamps.append(parsed_ts)
                    except:
                        continue
                
                if parsed_timestamps:
                    summary['earliest_timestamp'] = min(parsed_timestamps).isoformat()
                    summary['latest_timestamp'] = max(parsed_timestamps).isoformat()
            except Exception as timestamp_error:
                logging.debug(f"[{correlation_id}] Timestamp analysis failed: {str(timestamp_error)}")
        
        return summary
        
    except Exception as summary_error:
        logging.error(
            f"[{correlation_id}] Device summary generation failed: {str(summary_error)}",
            extra={'error_type': type(summary_error).__name__}
        )
        return {
            'total_devices': 0,
            'device_list': [],
            'total_consumption_readings': 0,
            'processing_timestamp': datetime.now(timezone.utc).isoformat(),
            'error': str(summary_error)
        }


# Add these missing function definitions to your existing file:

def _validate_business_data(business_data: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    ✅ LEGACY COMPATIBILITY: Legacy business data validation function.
    Following Azure Functions best practices for backward compatibility.
    Routes to the enhanced validation function for Device Map Structure support.
    """
    logging.debug(f"[{correlation_id}] Using legacy validation function - routing to enhanced validation")
    return _validate_business_data_preserve_original(business_data, correlation_id)


def _sanitize_field(field_value: str) -> str:
    """
    ✅ NEW: Sanitize individual string fields for security and data integrity.
    Following Azure Functions best practices for input sanitization and security.
    """
    try:
        if not isinstance(field_value, str):
            return str(field_value) if field_value is not None else ""
        
        # Remove null bytes and control characters (except newline, tab, carriage return)
        sanitized = ''.join(char for char in field_value if ord(char) >= 32 or char in '\n\t\r')
        
        # Trim whitespace
        sanitized = sanitized.strip()
        
        # Limit length to prevent memory issues
        max_length = 1000  # Configurable limit
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]
            logging.debug(f"Truncated field value from {len(field_value)} to {max_length} characters")
        
        # Basic XSS prevention - remove potentially dangerous characters
        dangerous_chars = ['<', '>', '"', "'", '&', '\x00']
        for char in dangerous_chars:
            if char in sanitized:
                sanitized = sanitized.replace(char, '')
                logging.debug(f"Removed dangerous character '{char}' from field")
        
        return sanitized
        
    except Exception as sanitize_error:
        logging.warning(f"Field sanitization failed: {str(sanitize_error)}, returning empty string")
        return ""


def _sanitize_data_recursive(data: Any) -> Any:
    """
    ✅ NEW: Recursively sanitize data structures while preserving original structure.
    Following Azure Functions best practices for comprehensive data cleaning.
    """
    try:
        if isinstance(data, dict):
            # Recursively sanitize dictionary values
            sanitized_dict = {}
            for key, value in data.items():
                # Sanitize the key if it's a string
                clean_key = _sanitize_field(str(key)) if isinstance(key, str) else key
                # Recursively sanitize the value
                sanitized_dict[clean_key] = _sanitize_data_recursive(value)
            return sanitized_dict
            
        elif isinstance(data, list):
            # Recursively sanitize list items
            return [_sanitize_data_recursive(item) for item in data]
            
        elif isinstance(data, str):
            # Sanitize string values
            return _sanitize_field(data)
            
        elif isinstance(data, (int, float, bool)) or data is None:
            # Preserve numeric, boolean, and None values as-is
            return data
            
        else:
            # For unknown types, convert to string and sanitize
            try:
                return _sanitize_field(str(data))
            except Exception:
                logging.warning(f"Failed to sanitize unknown data type: {type(data)}")
                return ""
                
    except Exception as recursive_sanitize_error:
        logging.error(f"Recursive sanitization failed: {str(recursive_sanitize_error)}")
        return data  # Return original data if sanitization fails


def _sanitize_event_data(event_data: Dict[str, Any]) -> None:
    """
    ✅ NEW: Sanitize EventGrid event data in-place for security.
    Following Azure Functions best practices for event data security.
    """
    try:
        # Sanitize critical EventGrid fields
        string_fields_to_sanitize = ['eventType', 'subject', 'dataVersion', 'metadataVersion']
        
        for field in string_fields_to_sanitize:
            if field in event_data and isinstance(event_data[field], str):
                original_value = event_data[field]
                event_data[field] = _sanitize_field(event_data[field])
                if original_value != event_data[field]:
                    logging.debug(f"Sanitized EventGrid field '{field}': '{original_value}' -> '{event_data[field]}'")
        
        # Ensure critical fields have safe values
        if 'eventType' in event_data and not event_data['eventType']:
            event_data['eventType'] = 'Unknown'
            
        if 'subject' in event_data and not event_data['subject']:
            event_data['subject'] = '/unknown'
            
        # Recursively sanitize the data payload
        if 'data' in event_data:
            event_data['data'] = _sanitize_data_recursive(event_data['data'])
            
    except Exception as event_sanitize_error:
        logging.error(f"Event data sanitization failed: {str(event_sanitize_error)}")


def _log_event_inspection(event: func.EventHubEvent, correlation_id: str, event_index: int) -> None:
    """
    ✅ NEW: Comprehensive event inspection logging for debugging and monitoring.
    Following Azure Functions best practices for detailed event analysis.
    """
    try:
        # Extract basic event properties
        event_properties = {
            'correlation_id': correlation_id,
            'event_index': event_index,
            'sequence_number': event.sequence_number,
            'offset': event.offset,
            'enqueued_time': event.enqueued_time.isoformat() if event.enqueued_time else None,
            'partition_key': event.partition_key,
            'content_type': getattr(event, 'content_type', 'unknown'),
            'message_id': getattr(event, 'message_id', 'unknown')
        }
        
        # Get event body safely
        try:
            event_body = event.get_body().decode('utf-8')
            body_length = len(event_body)
            body_preview = event_body[:300] + '...' if len(event_body) > 300 else event_body
            
            # Try to parse JSON for structure analysis
            try:
                parsed_body = json.loads(event_body)
                if isinstance(parsed_body, dict):
                    body_structure = {
                        'is_json': True,
                        'top_level_keys': list(parsed_body.keys()),
                        'estimated_device_count': _estimate_device_count(parsed_body),
                        'has_consumption_devices': 'Data' in parsed_body and 'ConsumptionDevices' in parsed_body.get('Data', {}),
                        'schema_type': _detect_schema_format(parsed_body)
                    }
                else:
                    body_structure = {'is_json': True, 'type': type(parsed_body).__name__}
            except json.JSONDecodeError:
                body_structure = {'is_json': False, 'parse_error': 'invalid_json'}
                
        except Exception as body_error:
            event_body = f"Error reading body: {str(body_error)}"
            body_length = 0
            body_preview = event_body
            body_structure = {'error': str(body_error)}
        
        # Log comprehensive event inspection
        logging.info(
            f"[{correlation_id}] Event {event_index} inspection",
            extra={
                'event_properties': event_properties,
                'body_length': body_length,
                'body_preview': body_preview,
                'body_structure': body_structure,
                'inspection_type': 'comprehensive_event_analysis'
            }
        )
        
        # Additional debug logging for Device Map Structure
        if isinstance(body_structure.get('top_level_keys'), list):
            if 'Data' in body_structure['top_level_keys']:
                logging.debug(
                    f"[{correlation_id}] Device Map Structure detected in event {event_index}",
                    extra={
                        'has_data_section': True,
                        'estimated_devices': body_structure.get('estimated_device_count', 0),
                        'schema_detection': body_structure.get('schema_type', 'unknown')
                    }
                )
        
    except Exception as inspection_error:
        logging.error(
            f"[{correlation_id}] Event inspection failed for event {event_index}: {str(inspection_error)}",
            extra={'error_type': type(inspection_error).__name__}
        )


def _estimate_device_count(parsed_data: Dict[str, Any]) -> int:
    """
    ✅ NEW: Estimate device count from parsed JSON for monitoring.
    Following Azure Functions best practices for data analysis.
    """
    try:
        # Check for Device Map Structure
        if isinstance(parsed_data.get('Data'), dict):
            consumption_devices = parsed_data['Data'].get('ConsumptionDevices', {})
            if isinstance(consumption_devices, dict):
                return len(consumption_devices)
        
        # Check for legacy single device structure
        if 'DeviceId' in parsed_data or 'CSDID' in parsed_data:
            return 1
        
        # Check for array-based structures
        for key, value in parsed_data.items():
            if isinstance(value, list) and key.lower() in ['devices', 'consumptiondevices']:
                return len(value)
        
        return 0
        
    except Exception:
        return 0


def _validate_business_data_with_diagnostics(business_data: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    ✅ NEW: Enhanced business data validation with comprehensive diagnostics.
    Following Azure Functions best practices for detailed validation tracking.
    """
    try:
        logging.info(
            f"[{correlation_id}] Starting business data validation with diagnostics",
            extra={
                'input_data_type': type(business_data).__name__,
                'input_keys': list(business_data.keys()) if isinstance(business_data, dict) else 'not_dict',
                'validation_type': 'diagnostic_validation'
            }
        )
        
        # Use the main validation function
        validated_data = _validate_business_data_preserve_original(business_data, correlation_id)
        
        # Add diagnostic information
        diagnostic_info = {
            'validation_timestamp': datetime.now(timezone.utc).isoformat(),
            'original_field_count': len(business_data) if isinstance(business_data, dict) else 0,
            'validated_field_count': len(validated_data) if isinstance(validated_data, dict) else 0,
            'has_device_map': 'Data' in validated_data and 'ConsumptionDevices' in validated_data.get('Data', {}),
            'device_count': len(validated_data.get('Data', {}).get('ConsumptionDevices', {})),
            'validation_success': True
        }
        
        logging.info(
            f"[{correlation_id}] Business data validation completed successfully",
            extra={
                'diagnostic_info': diagnostic_info,
                'final_device_id': validated_data.get('DeviceId'),
                'validation_type': 'diagnostic_complete'
            }
        )
        
        return validated_data
        
    except Exception as validation_error:
        logging.error(
            f"[{correlation_id}] Business data validation with diagnostics failed: {str(validation_error)}",
            extra={'error_type': type(validation_error).__name__}
        )
        # Return safe fallback data
        return {
            'id': str(uuid.uuid4()),
            'DeviceId': 'validation-failed',
            'CustomerId': 'validation-failed',
            'LocationSiteId': 'validation-failed',
            'EventType': 'validation-error',
            'Timestamp': datetime.now(timezone.utc).isoformat(),
            'Data': {'ConsumptionDevices': {}},
            'ValidationError': str(validation_error)
        }


def _safe_json_extract(json_str: str, fallback_value: Any = None) -> Any:
    """
    ✅ NEW: Safely extract JSON data with proper error handling.
    Following Azure Functions best practices for safe JSON processing.
    """
    try:
        if not isinstance(json_str, str) or not json_str.strip():
            return fallback_value
        
        parsed_data = json.loads(json_str)
        return parsed_data
        
    except json.JSONDecodeError as json_error:
        logging.warning(f"JSON decode error: {str(json_error)}")
        return fallback_value
    except Exception as extract_error:
        logging.error(f"JSON extraction error: {str(extract_error)}")
        return fallback_value


def _validate_uuid_format(value: str) -> bool:
    """
    ✅ NEW: Validate UUID format for IDs and CSDs.
    Following Azure Functions best practices for ID validation.
    """
    try:
        if not isinstance(value, str):
            return False
        
        # Try to parse as UUID
        uuid.UUID(value)
        return True
        
    except (ValueError, TypeError):
        return False


def _generate_safe_fallback_id(prefix: str = "fallback") -> str:
    """
    ✅ NEW: Generate safe fallback IDs when validation fails.
    Following Azure Functions best practices for reliable ID generation.
    """
    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        random_suffix = str(uuid.uuid4())[:8]
        return f"{prefix}_{timestamp}_{random_suffix}"
    except Exception:
        return f"{prefix}_{uuid.uuid4()}"


def _log_processing_metrics(correlation_id: str, stage: str, metrics: Dict[str, Any]) -> None:
    """
    ✅ NEW: Log processing metrics for monitoring and optimization.
    Following Azure Functions best practices for performance monitoring.
    """
    try:
        logging.info(
            f"[{correlation_id}] Processing metrics for stage: {stage}",
            extra={
                'correlation_id': correlation_id,
                'processing_stage': stage,
                'metrics': metrics,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'metric_type': 'processing_performance'
            }
        )
    except Exception as metrics_error:
        logging.error(f"[{correlation_id}] Failed to log metrics for {stage}: {str(metrics_error)}")


def _apply_data_type_coercion(value: Any, target_type: str) -> Any:
    """
    ✅ NEW: Safely coerce data types with fallback handling.
    Following Azure Functions best practices for type safety.
    """
    try:
        if target_type == 'string':
            return str(value) if value is not None else ""
        elif target_type == 'integer':
            return int(float(value)) if value is not None else 0
        elif target_type == 'float':
            return float(value) if value is not None else 0.0
        elif target_type == 'boolean':
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ['true', 'yes', '1', 'on']
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                return False
        else:
            return value
            
    except (ValueError, TypeError) as coercion_error:
        logging.debug(f"Type coercion failed for {target_type}: {str(coercion_error)}")
        # Return safe defaults
        defaults = {
            'string': "",
            'integer': 0,
            'float': 0.0,
            'boolean': False
        }
        return defaults.get(target_type, value)


# ✅ ADD: Import statements for additional modules if needed
import re
from typing import Union

def _extract_timestamp_from_data(data: Dict[str, Any]) -> Optional[str]:
    """
    ✅ NEW: Extract timestamp from various possible locations in data.
    Following Azure Functions best practices for flexible timestamp handling.
    """
    try:
        # Common timestamp field names to check
        timestamp_fields = [
            'Timestamp', 'timestamp', 'eventTime', 'time', 'created', 
            'updated', 'recorded', 'measured', 'DateTime', 'dateTime'
        ]
        
        # Check top-level fields first
        for field in timestamp_fields:
            if field in data and data[field]:
                return _validate_and_normalize_timestamp(data[field], "timestamp_extract", 0)
        
        # Check nested in Data section
        if 'Data' in data and isinstance(data['Data'], dict):
            for field in timestamp_fields:
                if field in data['Data'] and data['Data'][field]:
                    return _validate_and_normalize_timestamp(data['Data'][field], "timestamp_extract", 0)
        
        # Check in ConsumptionDevices (first device, first reading)
        consumption_devices = data.get('Data', {}).get('ConsumptionDevices', {})
        if isinstance(consumption_devices, dict):
            for device_data in consumption_devices.values():
                if isinstance(device_data, dict):
                    consumption_data = device_data.get('ConsumptionData', [])
                    if isinstance(consumption_data, list) and len(consumption_data) > 0:
                        first_reading = consumption_data[0]
                        if isinstance(first_reading, dict):
                            for field in timestamp_fields:
                                if field in first_reading and first_reading[field]:
                                    return _validate_and_normalize_timestamp(first_reading[field], "timestamp_extract", 0)
        
        # Return current timestamp if nothing found
        return datetime.now(timezone.utc).isoformat()
        
    except Exception as timestamp_error:
        logging.debug(f"Timestamp extraction failed: {str(timestamp_error)}")
        return datetime.now(timezone.utc).isoformat()      