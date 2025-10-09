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
    database_name="iseos-iot-db",  # Replace with your database name
    container_name="telemetry-data",  # Replace with your container name
    connection="CosmosDbConStr",
    create_if_not_exists=True
)
def EventGrid_Handler(azeventhub: List[func.EventHubEvent], cosmosout: func.Out[func.DocumentList]):
    """
    Azure Function to process EventGrid events from EventHub and store in Cosmos DB.
    
    Features:
    - Batch processing for better performance
    - Structured logging with correlation IDs
    - Input validation and sanitization
    - Error handling with retry-friendly patterns
    - Enhanced data transformation
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
        for event in azeventhub:
            try:
                # Extract event metadata
                event_metadata = {
                    'sequence_number': event.sequence_number,
                    'offset': event.offset,
                    'enqueued_time': event.enqueued_time.isoformat() if event.enqueued_time else None,
                    'partition_key': event.partition_key,
                    'correlation_id': correlation_id
                }
                
                # Parse event body with validation
                event_data = _parse_and_validate_event_data(event.get_body().decode('utf-8'), correlation_id)
                
                if event_data is None:
                    error_count += 1
                    continue
                
                # Transform data for Cosmos DB
                cosmos_document = _transform_eventgrid_data(event_data, event_metadata, correlation_id)
                
                if cosmos_document:
                    documents_to_insert.append(cosmos_document)
                    processed_count += 1
                    
                    # Log individual event processing (debug level to avoid noise)
                    logging.debug(f"[{correlation_id}] Processed event: {event.sequence_number}")
                
            except Exception as event_error:
                error_count += 1
                logging.error(
                    f"[{correlation_id}] Error processing individual event {event.sequence_number}: "
                    f"{str(event_error)}",
                    extra={
                        'correlation_id': correlation_id,
                        'sequence_number': event.sequence_number,
                        'error_type': type(event_error).__name__
                    }
                )
                continue
        
        # Bulk insert to Cosmos DB if we have documents
        if documents_to_insert:
            cosmosout.set(func.DocumentList(documents_to_insert))
            logging.info(
                f"[{correlation_id}] Successfully inserted {len(documents_to_insert)} documents to Cosmos DB"
            )
        
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
            extra={
                'correlation_id': correlation_id,
                'error_type': type(batch_error).__name__,
                'batch_size': batch_size
            }
        )
        # Re-raise for retry mechanisms
        raise


def _parse_and_validate_event_data(event_body: str, correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    Parse and validate EventGrid event data from EventHub.
    Validates both EventGrid schema and business data fields.
    """
    try:
        event_data = json.loads(event_body)
        
        if not isinstance(event_data, dict):
            logging.warning(f"[{correlation_id}] Event data is not a dictionary: {type(event_data)}")
            return None
        
        # Validate required EventGrid envelope fields (Microsoft EventGrid standard)
        required_eventgrid_fields = [
            'eventType',    # Event type (e.g., "Microsoft.Devices.DeviceTelemetry")
            'subject',      # Resource subject (e.g., "/devices/device-001")
            'data',         # Business data payload 
            'eventTime',    # ISO 8601 timestamp
            'id'            # Unique event identifier
        ]
        
        missing_eventgrid_fields = [field for field in required_eventgrid_fields if field not in event_data]
        
        if missing_eventgrid_fields:
            logging.warning(
                f"[{correlation_id}] Missing required EventGrid fields: {missing_eventgrid_fields}",
                extra={
                    'missing_fields': missing_eventgrid_fields, 
                    'available_fields': list(event_data.keys()),
                    'validation_type': 'eventgrid_schema'
                }
            )
            return None
        
        # Validate that data field contains business payload
        business_data = event_data.get('data', {})
        if not isinstance(business_data, dict):
            logging.warning(
                f"[{correlation_id}] EventGrid 'data' field is not a dictionary: {type(business_data)}",
                extra={'validation_type': 'business_data_format'}
            )
            return None
        
        # Apply business data validation (your CustomerId, LocationSiteId, DeviceId requirements)
        validated_business_data = _apply_required_field_validation(business_data, correlation_id)
        event_data['data'] = validated_business_data
        
        # Sanitize the complete event
        _sanitize_event_data(event_data)
        
        logging.debug(
            f"[{correlation_id}] EventGrid validation successful",
            extra={
                'event_type': event_data['eventType'],
                'subject': event_data['subject'],
                'validation_type': 'success'
            }
        )
        
        return event_data
        
    except json.JSONDecodeError as json_error:
        logging.error(
            f"[{correlation_id}] JSON decode error: {str(json_error)}",
            extra={'event_body_preview': event_body[:200] if event_body else 'Empty'}
        )
        return None
    except Exception as validation_error:
        logging.error(
            f"[{correlation_id}] EventGrid validation error: {str(validation_error)}",
            extra={'error_type': type(validation_error).__name__}
        )
        return None


def _transform_eventgrid_data(event_data: Dict[str, Any], event_metadata: Dict[str, Any], correlation_id: str) -> Optional[Dict[str, Any]]:
    """
    Transform EventGrid event data for Cosmos DB storage.
    Enhanced to use validated data with required fields.
    
    Args:
        event_data: Parsed and validated event data
        event_metadata: Event metadata from EventHub
        correlation_id: Correlation ID for logging
        
    Returns:
        Transformed document for Cosmos DB or None if transformation fails
    """
    try:
        # Extract validated data payload
        data_payload = event_data.get('data', {})
        
        # Create Cosmos DB document with validated required fields (matching your EventHub handler pattern)
        cosmos_document = {
            # Standard Cosmos DB fields
            'id': str(uuid.uuid4()),  # Unique document ID
            
            # Required business fields with validated defaults (matching your EventHub handler)
            'CustomerId': data_payload.get('CustomerId', 'default-customer'),        # Required for partition key
            'LocationSiteId': data_payload.get('LocationSiteId', 'default-location'), # Required for partition key
            'DeviceId': data_payload.get('DeviceId', 'default-device'),              # Required for device tracking
            
            # EventGrid specific fields
            'EventType': event_data.get('eventType'),      # Matches your EventHub handler pattern
            'Subject': event_data.get('subject'),
            'EventTime': event_data.get('eventTime'),
            'DataVersion': event_data.get('dataVersion'),
            'MetadataVersion': event_data.get('metadataVersion'),
            
            # Complete event data payload (matches your EventHub handler pattern)
            'Body': data_payload,
            
            # Processing metadata (matches your EventHub handler pattern)
            'Timestamp': datetime.now(timezone.utc).isoformat(),  # Matches your EventHub handler
            'CorrelationId': correlation_id,
            'Source': 'EventGrid',
            'FunctionName': 'EventGrid_Handler',
            
            # EventHub metadata (matches your EventHub handler pattern)
            'SequenceNumber': event_metadata.get('sequence_number'),  # Matches your EventHub handler
            'EventHub': {
                'offset': event_metadata.get('offset'),
                'enqueuedTime': event_metadata.get('enqueued_time'),
                'partitionKey': event_metadata.get('partition_key')
            },
            
            # Additional processing fields
            'ProcessingVersion': '1.0',
            'Environment': os.getenv('AZURE_FUNCTIONS_ENVIRONMENT', 'Development')
        }
        
        # Add custom transformations based on event type
        _apply_event_type_specific_transformations(cosmos_document, event_data, correlation_id)
        
        # Log successful transformation
        logging.debug(
            f"[{correlation_id}] Document transformed successfully",
            extra={
                'document_id': cosmos_document['id'],
                'customer_id': cosmos_document['CustomerId'],
                'device_id': cosmos_document['DeviceId'],
                'event_type': cosmos_document['EventType']
            }
        )
        
        return cosmos_document
        
    except Exception as transform_error:
        logging.error(
            f"[{correlation_id}] Data transformation error: {str(transform_error)}",
            extra={
                'error_type': type(transform_error).__name__,
                'event_type': event_data.get('eventType', 'unknown')
            }
        )
        return None


def _apply_event_type_specific_transformations(cosmos_document: Dict[str, Any], event_data: Dict[str, Any], correlation_id: str) -> None:
    """
    Apply event-type specific transformations.
    
    Args:
        cosmos_document: Document being prepared for Cosmos DB
        event_data: Original event data
        correlation_id: Correlation ID for logging
    """
    event_type = event_data.get('eventType', '')
    
    try:
        # Add transformations based on EventGrid event types
        if 'Microsoft.Devices.DeviceConnected' in event_type:
            cosmos_document['deviceConnectionStatus'] = 'Connected'
            cosmos_document['category'] = 'DeviceLifecycle'
            
        elif 'Microsoft.Devices.DeviceDisconnected' in event_type:
            cosmos_document['deviceConnectionStatus'] = 'Disconnected'
            cosmos_document['category'] = 'DeviceLifecycle'
            
        elif 'Microsoft.Devices.DeviceTelemetry' in event_type:
            cosmos_document['category'] = 'Telemetry'
            # Extract device ID if available
            if 'deviceId' in event_data.get('data', {}):
                cosmos_document['deviceId'] = event_data['data']['deviceId']
                
        elif 'Microsoft.EventGrid.SubscriptionValidationEvent' in event_type:
            cosmos_document['category'] = 'EventGridValidation'
            
        else:
            cosmos_document['category'] = 'Other'
            logging.debug(f"[{correlation_id}] Unhandled event type: {event_type}")
            
    except Exception as transformation_error:
        logging.warning(
            f"[{correlation_id}] Event type transformation failed: {str(transformation_error)}",
            extra={'event_type': event_type}
        )
        # Don't fail the entire transformation for this error
        cosmos_document['category'] = 'TransformationError'


def _apply_required_field_validation(data_payload: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """
    Simple validation with essential security sanitization.
    Follows Azure Functions best practices for input validation and sanitization.
    
    Args:
        data_payload: The 'data' section of the EventGrid event
        correlation_id: Correlation ID for logging
        
    Returns:
        Data payload with sanitized fields and defaults applied where needed
    """
    validated_data = data_payload.copy()
    
    # Sanitize and validate CustomerId
    customer_id = _safe_get_string(validated_data, 'CustomerId')
    if not customer_id:
        validated_data['CustomerId'] = 'default-customer'
        logging.error(f"[{correlation_id}] Missing or invalid CustomerId - applied default")
    else:
        validated_data['CustomerId'] = _sanitize_field(customer_id)
    
    # Sanitize and validate LocationSiteId  
    location_id = _safe_get_string(validated_data, 'LocationSiteId')
    if not location_id:
        validated_data['LocationSiteId'] = 'default-location'
        logging.error(f"[{correlation_id}] Missing or invalid LocationSiteId - applied default")
    else:
        validated_data['LocationSiteId'] = _sanitize_field(location_id)
    
    # Sanitize and validate DeviceId
    device_id = _safe_get_string(validated_data, 'DeviceId')
    if not device_id:
        validated_data['DeviceId'] = 'default-device'
        logging.error(f"[{correlation_id}] Missing or invalid DeviceId - applied default")
    else:
        validated_data['DeviceId'] = _sanitize_field(device_id)
    
    # Sanitize the entire Body for security (recursive sanitization)
    validated_data = _sanitize_data_recursive(validated_data)
    
    return validated_data


def _safe_get_string(data: Dict[str, Any], field_name: str) -> str:
    """
    Safely extract string field with type validation.
    
    Args:
        data: Dictionary to extract from
        field_name: Field name to extract
        
    Returns:
        String value or empty string if invalid
    """
    value = data.get(field_name, '')
    if not isinstance(value, str):
        return str(value) if value is not None else ''
    return value.strip()


def _sanitize_field(value: str) -> str:
    """
    Lightweight field sanitization following Azure Functions security best practices.
    
    Args:
        value: String value to sanitize
        
    Returns:
        Sanitized string value
    """
    if not isinstance(value, str):
        value = str(value)
    
    # Remove dangerous characters for security
    sanitized = value.replace('\x00', '')      # Null bytes (injection prevention)
    sanitized = sanitized.replace('\r', '')    # Carriage returns (log injection prevention) 
    sanitized = sanitized.replace('\n', '')    # Newlines (log injection prevention)
    sanitized = sanitized.replace('\t', ' ')   # Tabs to spaces (normalization)
    
    # Remove other control characters that could cause issues
    sanitized = ''.join(char for char in sanitized if ord(char) >= 32 or char == ' ')
    
    # Trim whitespace and limit length for DoS prevention
    sanitized = sanitized.strip()[:255]
    
    return sanitized


def _sanitize_data_recursive(data: Any) -> Any:
    """
    Recursively sanitize data structures for comprehensive security.
    
    Args:
        data: Data to sanitize (dict, list, str, or other)
        
    Returns:
        Sanitized data structure
    """
    if isinstance(data, dict):
        return {key: _sanitize_data_recursive(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [_sanitize_data_recursive(item) for item in data]
    elif isinstance(data, str):
        return _sanitize_field(data)
    else:
        return data


def _sanitize_event_data(event_data: Dict[str, Any]) -> None:
    """
    Sanitize event data in-place with enhanced security.
    Updated to follow Azure Functions security best practices.
    
    Args:
        event_data: Event data dictionary to sanitize
    """
    # Sanitize the entire event data structure
    for key, value in event_data.items():
        event_data[key] = _sanitize_data_recursive(value)