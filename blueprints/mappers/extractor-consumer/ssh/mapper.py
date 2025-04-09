from  __future__  import  annotations

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__name__)))))

import base64
import logging

import ies_tool.ies_tool as ies

from json import dumps
from time import perf_counter
from logging import StreamHandler

from telicent_lib import Record
from telicent_lib import RecordUtils
from telicent_lib.config import Configurator
from telicent_lib.sources import KafkaSource
from telicent_lib.logging import CoreLoggerFactory
from telicent_lib.sinks import KafkaSink
from telicent_lib import Mapper

from mapping_function import azure_extraction

from dotenv import load_dotenv


# Mapper Configuration
load_dotenv()
config = Configurator()
source_topic = config.get("SOURCE_TOPIC", required=True,
                    description="Specifies the Kafka topic the mapper ingests from.")
target_topic = config.get("TARGET_TOPIC", required=True,
                    description="Specifies the Kafka topic the mapper pushes its output to")

default_security_label = "nationality=GBR"

logger = logging.getLogger(__name__)

tool = ies.IESTool(mode="rdflib")


def get_headers():
    output = RecordUtils.to_headers(
        {
            "Content-Type": "text/turtle"
        }
    )
    logger.debug(output)
    return output

def mapping_function(record: Record) ->  Record | list[Record] | None:

    start_mapping = perf_counter()
    logger.info("Mapping Started...")

    data = record.value
    logger.debug(data)

    azure_extraction(data)
    mapped_record = Record(
        get_headers(), 
        record.key, 
        data, 
        None
    )

    logger.info(f"Completed mapping of item in: {str(round(perf_counter()-start_mapping,3))}s")
    
    return mapped_record
    

if __name__ == "__main__":

    # Obtain the producer and consumer connection strings
    eventhub_conn_str = base64.b64decode(os.environ['EVENTHUB_CONNECTION_STRING']).decode('utf-8').replace('\n', '')

    # Define kafka_configs
    consumer_configs = {
        'bootstrap.servers': config.get('BOOTSTRAP_SERVERS', required=True, description="Specifies the eventhub bootstrap servers"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': eventhub_conn_str,
        'auto.offset.reset': 'earliest',
        'group.id': '$Default',
    }

    producer_configs = {
        'bootstrap.servers': config.get('BOOTSTRAP_SERVERS', required=True, description="Specifies the eventhub bootstrap servers"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': eventhub_conn_str,
    }

    # Define the source and target
    source = KafkaSource(topic=source_topic, kafka_config=consumer_configs, commit_interval=5)    
    target = KafkaSink(topic=target_topic, kafka_config=producer_configs)    

    # Define the mapper
    mapper = Mapper(
        source=source, 
        target=target, 
        map_function=mapping_function, 
        name=source_topic + " to " + target_topic + " Mapper"
    )

    # Run the mapper    
    mapper.run()
    logger.info("Mapper Running...")