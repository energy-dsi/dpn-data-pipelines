# Script containing the mapping function for the azure extraction mapper.

import os
import base64
import logging

from telicent_lib.config import Configurator

from azure.storage.blob import BlobServiceClient

from dotenv import load_dotenv

load_dotenv()
config = Configurator()

logger = logging.getLogger(__name__)

def azure_extraction(item):

    connection_string = base64.b64decode(os.environ['BLOB_CONNECTION_STRING']).decode('utf-8').replace('\n', '')
    container_name = os.environ['CONTAINER_NAME']
    folder_name = os.environ['FOLDER_NAME']
    file_name = os.environ['FILE_NAME']

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f'{folder_name}/{file_name}')
    blob_client.upload_blob(item, overwrite=True)

    print(f"XML file uploaded successfully to {folder_name}/{file_name}")
