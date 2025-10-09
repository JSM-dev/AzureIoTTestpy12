import azure.functions as func
import logging
import json
import datetime
import os
import uuid


bp_eventhubHandler = func.Blueprint()


@bp_eventhubHandler.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name='EventHubName',
    connection="EventHubConStr",
    consumer_group='$Default'
)
@bp_eventhubHandler.cosmos_db_output(
    arg_name="cosmosout",
    database_name=os.environ.get('CosmosDbDatabase', 'EOKS-db-prod'),
    container_name=os.environ.get('CosmosDbContainer', 'Container1'),
    connection="CosmosDbConnectionString"
)

def ISEOS_iot_Handler(azeventhub: func.EventHubEvent, cosmosout: func.Out[func.Document]):
    message = azeventhub.get_body().decode('utf-8')
    logging.info('Python EventHub trigger psrocessed an event: %s', message)

    # Get environment variables
    try:
        COSMOS_CONNECTION_STRING = os.environ.get('CosmosDbConnectionString')
        COSMOS_DB_NAME = os.environ.get('CosmosDbDatabase', 'IoTEvents')
        COSMOS_CONTAINER_NAME = os.environ.get('CosmosDbContainer', 'Events')
    except Exception as e:
        logging.error(f"Error retrieving environment variables: {str(e)}")
        return

    try:   
    # THIS SHOULD PRINT YOUR DB NAME!
        logging.info('CONTAINER NAME: %s', COSMOS_CONTAINER_NAME)
    except Exception as e:
        logging.error(f"Error logging DB or container name: {str(e)}")
        return

    try:   
    # THIS SHOULD PRINT YOUR DB NAME!
        logging.info('CONTAINER NAME: %s', COSMOS_DB_NAME)
    except Exception as e:
        logging.error(f"Error logging COSMOS_DB_NAME")
        return


    if COSMOS_CONNECTION_STRING:
        logging.info('Cosmos connection string is available')
    else:
        logging.error('Cosmos connection string is MISSING from Application Settings!')

    # Using Azure Functions Cosmos DB Output Binding (no SDK import needed!)
    logging.info("Using Cosmos DB output binding - no package import required")


    try:
        logging.error('raw log of message: %s', message)
    except Exception as e:
        logging.error('Exception during raw log of message')
    # Prepare document to insert
    try:
        event_data = json.loads(message)
    except Exception:
        event_data = {"raw": 'Exception during json.loads or was flat',
                       "rawMessage": message}

    doc = {
        "id": str(uuid.uuid4()),  # Generate unique GUID for document ID
        "SequenceNumber": azeventhub.sequence_number if hasattr(azeventhub, 'sequence_number') else None,
        "CustomerId": event_data.get("CustomerId", "default-customer"),  # Required for partition key
        "LocationSiteId": event_data.get("LocationSiteId", "default-location"),  # Required for partition key
        "EventType": event_data.get("type", "generic"),
        "DeviceId": event_data.get("DeviceId", "default-device"),
        "Body": event_data,
        "Timestamp": datetime.datetime.utcnow().isoformat()
    }

    # Write to Cosmos DB using output binding
    logging.info('Prepared document for Cosmos DB: %s', doc)
    
    try:
        # This WILL write to Cosmos DB when the function completes!
        cosmosout.set(func.Document.from_dict(doc))
        logging.info("Document queued for Cosmos DB write: %s", doc["id"])
    except Exception as e:
        logging.error(f"Error setting Cosmos DB document: {str(e)}")
        # Try with minimal document as fallback
        minimal_doc = {"id": str(datetime.datetime.utcnow().timestamp()), "test": "data"}
        cosmosout.set(func.Document.from_dict(minimal_doc))
        logging.info("Tried with minimal document instead")

