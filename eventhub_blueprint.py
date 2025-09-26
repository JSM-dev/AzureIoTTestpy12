import azure.functions as func
import logging
import json
import datetime
import os


bp_eventhub = func.Blueprint()


@bp_eventhub.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name='EventHubName',
    connection="EventHubConStr",
    consumer_group='$Default'
)

def ISEOS_iot_Handler(azeventhub: func.EventHubEvent):
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

    # Connect to Cosmos DB using connection string (lazy import)
    try:
        from azure.cosmos import CosmosClient
        client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
        database = client.get_database_client(COSMOS_DB_NAME)
        container = database.get_container_client(COSMOS_CONTAINER_NAME)
    except ImportError as e:
        logging.error(f"Failed to import azure.cosmos: {str(e)}")
        return
    except Exception as e:
        logging.error(f"Cosmos DB database or container not found: {str(e)}")
        return

    # Prepare document to insert
    try:
        event_data = json.loads(message)
    except Exception:
        event_data = {"raw": 'Exception during json.loads'}

    doc = {
        "id": azeventhub.sequence_number if hasattr(azeventhub, 'sequence_number') else str(datetime.datetime.utcnow().timestamp()),
        "eventType": event_data.get("type", "generic"),
        "body": event_data,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

    # Write to Cosmos DB
    try:
        container.upsert_item(doc)
        logging.info("Event written to Cosmos DB: %s", doc["id"])
    except Exception as e:
        logging.error("Failed to write event to Cosmos DB: %s", str(e))
