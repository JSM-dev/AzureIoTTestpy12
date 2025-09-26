import azure.functions as func
import datetime
import json
import os
import logging
# import requests
# Cosmos DB imports
# from azure.cosmos import CosmosClient

from azure.functions import Blueprint, HttpRequest, HttpResponse

from httpfunc import bp_httpfunc
from eventhub_blueprint import bp_eventhub 

# Register all blueprints from an array
app = func.FunctionApp()
blueprints = [bp_eventhub, bp_httpfunc]
for bp in blueprints:
    app.register_blueprint(bp)


