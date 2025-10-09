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
    database_name="iseos-iot-db",
    container_name="telemetry-data",
    connection="CosmosDbConStr",
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
        
        # Extract and validate business data
        business_data = event_data.get('data', {})
        validated_business_data = _validate_business_data(business_data, correlation_id)
        
        # Convert to EventGrid format
        eventgrid_event = {
            'eventType': event_data.get('type', 'Microsoft.EventGrid.CloudEvent'),
            'subject': event_data.get('source', '/unknown'),
            'data': validated_business_data,
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


def _validate_business_data(business_data: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    Optimized business data validation with required field defaults.
    """
    validated_data = business_data.copy()
    
    # Ensure required fields with defaults
    field_defaults = {
        'id': str(uuid.uuid4()),
        'SequenceNumber': 0,
        'CustomerId': 'default-customer',
        'LocationSiteId': 'default-location',
        'DeviceId': 'default-device',
        'EventType': 'generic',
        'Timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    for field, default_value in field_defaults.items():
        if field not in validated_data or not validated_data[field]:
            validated_data[field] = default_value
            logging.debug(f"[{correlation_id}] Applied default for {field}")
    
    # Sanitize string fields
    string_fields = ['CustomerId', 'LocationSiteId', 'DeviceId', 'EventType']
    for field in string_fields:
        if isinstance(validated_data[field], str):
            validated_data[field] = _sanitize_field(validated_data[field])
    
    # Validate nested Data structure
    if 'Data' in validated_data and isinstance(validated_data['Data'], dict):
        nested_data = validated_data['Data']
        if 'DeviceId' not in nested_data:
            nested_data['DeviceId'] = validated_data['DeviceId']
        
        # Ensure consumption and network data structures exist
        if 'ConsumptionData' not in nested_data or not isinstance(nested_data['ConsumptionData'], dict):
            nested_data['ConsumptionData'] = {}
        if 'NetworkAnalyser' not in nested_data or not isinstance(nested_data['NetworkAnalyser'], dict):
            nested_data['NetworkAnalyser'] = {}
    else:
        validated_data['Data'] = {}
    
    return _sanitize_data_recursive(validated_data)


def _transform_to_cosmos_document(event_data: Dict[str, Any], event_metadata: Dict[str, Any], correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    Optimized transformation to Cosmos DB document format.
    """
    try:
        data_payload = event_data.get('data', {})
        
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
            
            # Complete payload and metadata
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
            'ProcessingVersion': '1.0',
            'Environment': os.getenv('AZURE_FUNCTIONS_ENVIRONMENT', 'Development')
        }
        
        logging.debug(
            f"[{correlation_id}] Document transformed successfully",
            extra={
                'document_id': cosmos_document['id'],
                'customer_id': cosmos_document['CustomerId'],
                'device_id': cosmos_document['DeviceId']
            }
        )
        
        return cosmos_document
        
    except Exception as transform_error:
        logging.error(
            f"[{correlation_id}] Document transformation failed: {str(transform_error)}",
            extra={'error_type': type(transform_error).__name__}
        )
        return None


def _log_event_inspection(event: func.EventHubEvent, correlation_id: str, event_index: int) -> None:
    """
    Optimized event inspection logging for debugging.
    """
    try:
        raw_body = event.get_body().decode('utf-8')
        
        # Basic payload analysis
        payload_info = {'raw_length': len(raw_body), 'is_valid_json': False}
        
        try:
            parsed_payload = json.loads(raw_body)
            if isinstance(parsed_payload, dict):
                schema_format = _detect_schema_format(parsed_payload)
                payload_info.update({
                    'is_valid_json': True,
                    'schema_format': schema_format,
                    'total_keys': len(parsed_payload.keys()),
                    'top_level_keys': list(parsed_payload.keys())[:10]  # First 10 keys
                })
        except json.JSONDecodeError:
            payload_info['json_error'] = 'Invalid JSON'
        
        logging.info(
            f"[{correlation_id}] Event {event_index + 1} inspection",
            extra={
                'correlation_id': correlation_id,
                'event_index': event_index,
                'sequence_number': event.sequence_number,
                'payload_info': payload_info,
                'raw_preview': _sanitize_field(raw_body[:300])  # First 300 chars
            }
        )
        
    except Exception as inspection_error:
        logging.error(
            f"[{correlation_id}] Event inspection failed: {str(inspection_error)}",
            extra={'correlation_id': correlation_id, 'event_index': event_index}
        )


def _sanitize_field(value: str) -> str:
    """
    Optimized field sanitization for security.
    """
    if not isinstance(value, str):
        value = str(value)
    
    # Remove dangerous characters
    sanitized = value.replace('\x00', '').replace('\r', '').replace('\n', '').replace('\t', ' ')
    sanitized = ''.join(char for char in sanitized if ord(char) >= 32 or char == ' ')
    
    return sanitized.strip()[:255]  # Limit length


def _sanitize_data_recursive(data: Any) -> Any:
    """
    Optimized recursive data sanitization.
    """
    if isinstance(data, dict):
        return {key: _sanitize_data_recursive(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_sanitize_data_recursive(item) for item in data[:100]]  # Limit list size
    elif isinstance(data, str):
        return _sanitize_field(data)
    else:
        return data


def _sanitize_event_data(event_data: Dict[str, Any]) -> None:
    """
    Optimized in-place event data sanitization.
    """
    for key, value in event_data.items():
        event_data[key] = _sanitize_data_recursive(value)