import azure.functions as func
from azure.functions import Blueprint
import logging
import json
import datetime
from azure.cosmos import CosmosClient
import os


bp_eventhub = Blueprint()


@bp_eventhub.event_hub_message_trigger(
    arg_name="azeventhub",
    event_hub_name='EventHubName',
    connection="EventHubConStr",
    consumer_group='$Default'
)

def ISEOS_iot_Handler(azeventhub: func.EventHubEvent):
    message = azeventhub.get_body().decode('utf-8')
    logging.info('Python EventHub trigger psrocessed an event: %s', message)
    COSMOS_CONNECTION_STRING = os.environ.get('CosmosDbConnectionString')
    COSMOS_DB_NAME = os.environ.get('CosmosDbDatabase', 'IoTEvents')
    COSMOS_CONTAINER_NAME = os.environ.get('CosmosDbContainer', 'Events')
    logging.info('Python EventHub trigger psrocessed an event: %s', COSMOS_CONNECTION_STRING,COSMOS_DB_NAME,COSMOS_CONTAINER_NAME)

    # Connect to Cosmos DB using connection string
    # client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)
    # try:
    #     database = client.get_database_client(COSMOS_DB_NAME)
    #     container = database.get_container_client(COSMOS_CONTAINER_NAME)
    # except Exception as e:
    #     logging.error(f"Cosmos DB database or container not found: {str(e)}")
        # return

    # # Prepare document to insert
    # try:
    #     event_data = json.loads(message)
    # except Exception:
    #     event_data = {"raw": 'Exception during json.loads'}

    # doc = {
    #     "id": azeventhub.sequence_number if hasattr(azeventhub, 'sequence_number') else str(datetime.datetime.utcnow().timestamp()),
    #     "eventType": event_data.get("type", "generic"),
    #     "body": event_data,
    #     "timestamp": datetime.datetime.utcnow().isoformat()
    # }

    # # Write to Cosmos DB
    # try:
    #     container.upsert_item(doc)
    #     logging.info("Event written to Cosmos DB: %s", doc["id"])
    # except Exception as e:
    #     logging.error("Failed to write event to Cosmos DB: %s", str(e))
