import azure.functions as func
import datetime
import json
import os
import logging

from azure.functions import Blueprint, HttpRequest, HttpResponse
from Azurec2dApi import  bp_c2dAPI
from eventhub_blueprint import bp_eventhubHandler 
from CosmosDbApi import bp_cosmosAPI
from eventhub_EventGridHandler import bp_eventgridHandler


# Register all blueprints from an array
app = func.FunctionApp()
blueprints = [bp_eventhubHandler, bp_c2dAPI, bp_cosmosAPI, bp_eventgridHandler]
for bp in blueprints:
    app.register_blueprint(bp)


