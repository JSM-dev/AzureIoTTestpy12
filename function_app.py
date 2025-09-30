import azure.functions as func
import datetime
import json
import os
import logging

from azure.functions import Blueprint, HttpRequest, HttpResponse
from Azurec2dApi import  bp_c2dAPI
from eventhub_blueprint import bp_eventhub 
from CosmosDbApi import bp_cosmosAPI

# Register all blueprints from an array
app = func.FunctionApp()
blueprints = [bp_eventhub, bp_c2dAPI, bp_cosmosAPI]
for bp in blueprints:
    app.register_blueprint(bp)


