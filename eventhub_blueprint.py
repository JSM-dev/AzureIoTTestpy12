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
@bp_eventhub.cosmos_db_output(
    arg_name="cosmosout",
    database_name="CosmosDbDatabase",
    container_name="CosmosDbContainer", 
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

    # Write to Cosmos DB using output binding (no try/catch needed!)
    cosmosout.set(func.Document.from_dict(doc))
    logging.info("Event written to Cosmos DB via binding: %s", doc["id"])
