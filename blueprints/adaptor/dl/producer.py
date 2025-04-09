# Script containing the adaptor code.

import os
import sys
import base64

sys.path.append(os.path.dirname(os.path.abspath(__name__)))

import logging

from typing import Iterable

from dotenv import load_dotenv

from utils import get_blob_object

from telicent_lib import Record
from telicent_lib import RecordUtils
from telicent_lib.config import Configurator
from telicent_lib.sinks import KafkaSink

load_dotenv()
config = Configurator()
target_topic = config.get(
    "TARGET_TOPIC",
    required=True,
    description="Specifies the Kafka topic the adaptor pushes its output to",
)
name = config.get(
    "PRODUCER_NAME", required=True, description="Specifies the name of the producer"
)
source_name = config.get(
    "SOURCE_NAME",
    required=True,
    description="Specifies the source that the data has originated from",
)


# Create a logger
logger = logging.getLogger(__name__)

# Define the kafka configurations
connection_string = base64.b64decode(os.environ['EVENTHUB_CONNECTION_STRING']).decode('utf-8').replace('\n', '')

kafka_config = {
    'bootstrap.servers': config.get('BOOTSTRAP_SERVERS', required=True, description="Specifies the eventhub bootstrap servers"),
    'sasl.username': '$ConnectionString',
    'sasl.password': connection_string,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
}

# Get the blob credential
credential = base64.b64decode(os.environ['BLOB_SAS_TOKEN']).decode('utf-8').replace('\n', '')

def create_record(data):
    return Record(
        RecordUtils.to_headers(
            {
                "Content-Type": "application/rdf+xml",
                "Data-Source": source_name,
                "Data-Producer": name,
            }
        ),
        None,
        data,
    )

def generate_records() -> Iterable[Record]:
    data = get_blob_object(
        account_url=config.get(
            'STORAGE_ACCOUNT_URL',
            required=True,
            description="Specifies the storage account url"
        ),
        credential=credential,
        container_name='[CONTAINER_NAME]',
        blob_name='DL.xml'
    )
    return create_record(data)

if __name__ == "__main__":

    # Generate the message content
    message_content = generate_records()

    # Define the landing zone
    sink = KafkaSink(topic=target_topic, kafka_config=kafka_config)
    logger.info("Connection to the kafka cluster established")

    # Send message to the topic
    sink.send(message_content)
    logger.info("Message sent to the topic")
