import azure.functions as func
import json
import logging
import os
import uuid
import base64
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

bp_eventgridHandler = func.Blueprint()

# AZURE FUNCTIONS BEST PRACTICE: Pre-compiled patterns for performance
DANGEROUS_CHARS = set('<>"\';&')

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
def EventGrid_Handler_Optimized(azeventhub: List[func.EventHubEvent], cosmosout: func.Out[func.DocumentList]):
    """
    HIGH-PERFORMANCE: Azure Functions EventGrid handler optimized for maximum efficiency.
    Following Azure Functions best practices for cold start optimization and memory efficiency.
    """
    
    correlation_id = str(uuid.uuid4())[:8]
    batch_size = len(azeventhub)
    
    # AZURE FUNCTIONS BEST PRACTICE: Minimal initial logging
    logging.info(f"[{correlation_id}] Processing {batch_size} events (High-performance)")
    
    # PERFORMANCE: Use dynamic list instead of pre-allocated array to avoid index errors
    documents = []
    
    # PERFORMANCE: Single counter instead of multiple variables
    stats = {'success': 0, 'errors': 0, 'base64': 0, 'missing_fields': 0}
    
    # AZURE FUNCTIONS BEST PRACTICE: Single-pass processing with minimal overhead
    for i, event in enumerate(azeventhub):
        event_correlation = f"{correlation_id}_{i}"
        
        try:
            # PERFORMANCE: Single JSON parse with immediate CloudEvents detection
            raw_data = json.loads(event.get_body().decode('utf-8'))
            
            # OPTIMIZED: Fast CloudEvents detection and payload extraction in one step
            payload, is_base64 = _extract_payload_fast(raw_data)
            
            if payload is not None:
                # PERFORMANCE: Track stats efficiently
                if is_base64:
                    stats['base64'] += 1
                
                # AZURE FUNCTIONS BEST PRACTICE: Input validation with error logging
                doc, has_missing_fields = _create_document_with_validation(payload, raw_data, event, event_correlation)
                
                if doc is not None:
                    # FIXED: Use append instead of index assignment to avoid bounds errors
                    documents.append(doc)
                    stats['success'] += 1
                    
                    if has_missing_fields:
                        stats['missing_fields'] += 1
                else:
                    stats['errors'] += 1
            else:
                stats['errors'] += 1
                
        except Exception as e:
            # AZURE FUNCTIONS BEST PRACTICE: Proper error handling and logging
            stats['errors'] += 1
            logging.error(f"[{event_correlation}] Processing error: {str(e)}")
    
    # AZURE FUNCTIONS BEST PRACTICE: Bulk insert with error handling
    if documents:
        try:
            cosmosout.set(func.DocumentList(documents))
            # PERFORMANCE: Single success log instead of per-document logging
            logging.info(f"[{correlation_id}] ✅ Queued {len(documents)} documents")
        except Exception as cosmos_error:
            logging.error(f"[{correlation_id}] ❌ Cosmos error: {str(cosmos_error)}")
    
    # AZURE FUNCTIONS BEST PRACTICE: Implement monitoring and health checks
    success_rate = (stats['success'] / batch_size * 100) if batch_size > 0 else 0
    
    # Enhanced logging with missing fields tracking
    logging.info(
        f"[{correlation_id}] Complete: {stats['success']}/{batch_size} success "
        f"({success_rate:.1f}%), {stats['base64']} base64, {stats['errors']} errors, "
        f"{stats['missing_fields']} missing fields"
    )
    
    # AZURE FUNCTIONS BEST PRACTICE: Log data quality issues for monitoring
    if stats['missing_fields'] > 0:
        logging.warning(
            f"[{correlation_id}] Data quality alert: {stats['missing_fields']} documents had missing required fields",
            extra={
                'correlation_id': correlation_id,
                'missing_fields_count': stats['missing_fields'],
                'total_events': batch_size,
                'missing_fields_percentage': (stats['missing_fields'] / batch_size * 100) if batch_size > 0 else 0
            }
        )


def _extract_payload_fast(raw_data: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    HIGH-PERFORMANCE: Fast payload extraction optimized for Azure EventGrid CloudEvents.
    Following Azure Functions best practices for minimal overhead and single-pass processing.
    """
    # PERFORMANCE: Fast type check and CloudEvents detection in one step
    if not isinstance(raw_data, dict) or 'specversion' not in raw_data:
        # PERFORMANCE: Early return for non-CloudEvents
        raw_data['_is_cloudevents'] = False
        return raw_data, False
    
    # OPTIMIZED: CloudEvents detected - extract payload efficiently
    payload = None
    is_base64 = False
    
    # PERFORMANCE: Check data_base64 first (most common for Azure EventGrid)
    data_base64 = raw_data.get('data_base64')
    if data_base64:
        try:
            # PERFORMANCE: Direct base64 decode without intermediate validation
            decoded_bytes = base64.b64decode(data_base64)
            payload = json.loads(decoded_bytes.decode('utf-8'))
            is_base64 = True
        except Exception:
            # PERFORMANCE: Silent failure - try data field as fallback
            pass
    
    # PERFORMANCE: Fallback to data field if base64 failed
    if payload is None:
        payload = raw_data.get('data')
    
    # OPTIMIZED: Add CloudEvents metadata efficiently if payload exists
    if payload is not None and isinstance(payload, dict):
        # PERFORMANCE: Single dictionary operation instead of multiple assignments
        payload.update({
            '_cloudevents_metadata': {
                'source': raw_data.get('source'),
                'type': raw_data.get('type'),
                'time': raw_data.get('time'),
                'subject': raw_data.get('subject'),
                'datacontenttype': raw_data.get('datacontenttype')
            },
            '_is_cloudevents': True,
            '_base64_decoded': is_base64
        })
    
    return payload, is_base64


def _create_document_with_validation(payload: Dict[str, Any], original_event: Dict[str, Any], event: func.EventHubEvent, event_correlation: str) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    HIGH-PERFORMANCE: Fast document creation with required field validation and error logging.
    Following Azure Functions best practices for input validation and proper error handling.
    """
    # PERFORMANCE: Fast type check
    if not isinstance(payload, dict):
        logging.error(f"[{event_correlation}] Invalid payload type: {type(payload).__name__}")
        return None, False
    
    # AZURE FUNCTIONS BEST PRACTICE: Validate required business fields with error logging
    missing_fields = []
    has_missing_fields = False
    
    # Extract required fields and track missing ones
    device_id_raw = payload.get('DeviceId')
    customer_id_raw = payload.get('CustomerId')
    location_site_id_raw = payload.get('LocationSiteId')
    
    # AZURE FUNCTIONS BEST PRACTICE: Proper input validation with structured logging
    if not device_id_raw or device_id_raw in ['', 'unknown-device']:
        missing_fields.append('DeviceId')
        has_missing_fields = True
    
    if not customer_id_raw or customer_id_raw in ['', 'default-customer']:
        missing_fields.append('CustomerId')
        has_missing_fields = True
    
    if not location_site_id_raw or location_site_id_raw in ['', 'default-location']:
        missing_fields.append('LocationSiteId')
        has_missing_fields = True
    
    # AZURE FUNCTIONS BEST PRACTICE: Structured error logging for missing fields
    if missing_fields:
        logging.warning(
            f"[{event_correlation}] Missing required fields: {', '.join(missing_fields)}",
            extra={
                'correlation_id': event_correlation,
                'missing_fields': missing_fields,
                'available_fields': list(payload.keys()),
                'device_id_present': 'DeviceId' in payload,
                'customer_id_present': 'CustomerId' in payload,
                'location_site_id_present': 'LocationSiteId' in payload
            }
        )
    
    # OPTIMIZED: Single-pass field extraction and sanitization with fallbacks
    device_id = _sanitize_field_fast(device_id_raw or 'unknown-device')
    customer_id = _sanitize_field_fast(customer_id_raw or 'default-customer')
    location_site_id = _sanitize_field_fast(location_site_id_raw or 'default-location')
    
    # PERFORMANCE: Fast device count calculation
    consumption_devices = payload.get('Data', {}).get('ConsumptionDevices', {})
    device_count = len(consumption_devices) if isinstance(consumption_devices, dict) else 0
    
    # OPTIMIZED: Create document with single dictionary operation
    cloudevents_meta = payload.get('_cloudevents_metadata', {})
    
    document = {
        'id': str(uuid.uuid4()),
        
        # PERFORMANCE: Core fields for efficient indexing
        'CustomerId': customer_id,
        'LocationSiteId': location_site_id,
        'DeviceId': device_id,
        'DeviceCount': device_count,
        
        # AZURE FUNCTIONS BEST PRACTICE: Track data quality for monitoring
        'MissingRequiredFields': missing_fields if missing_fields else None,
        'HasMissingFields': has_missing_fields,
        
        # PERFORMANCE: CloudEvents metadata in single operation
        'Source': cloudevents_meta.get('source'),
        'EventTime': cloudevents_meta.get('time'),
        'Subject': cloudevents_meta.get('subject'),
        'EventType': cloudevents_meta.get('type'),
        'DataContentType': cloudevents_meta.get('datacontenttype'),
        
        # PRESERVE: Complete data as requested
        'Data': payload.get('Data', {}),
        
        # PERFORMANCE: Minimal processing metadata
        'Timestamp': datetime.now(timezone.utc).isoformat(),
        'SequenceNumber': event.sequence_number,
        'Base64Decoded': payload.get('_base64_decoded', False),
        'ProcessingVersion': 'validated-v1.0'
    }
    
    return document, has_missing_fields


def _sanitize_field_fast(value: Any) -> str:
    """
    HIGH-PERFORMANCE: Fast field sanitization with minimal overhead.
    Following Azure Functions best practices for efficient string processing.
    """
    # PERFORMANCE: Fast string conversion and length limit
    str_value = str(value)[:50]
    
    # OPTIMIZED: Fast character filtering using set membership
    return ''.join(char for char in str_value if char not in DANGEROUS_CHARS)