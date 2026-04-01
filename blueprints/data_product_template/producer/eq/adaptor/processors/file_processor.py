import os

from dotenv import load_dotenv

from utils.data_transection import DataTransection
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()


class EQFileProcess:
    def __init__(self):
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("targetMapperTopicName")
        self.process_type = os.getenv("processType")
        self.target_file_name = os.getenv("targetMapperFileName")
        self.azure_conn_str = os.getenv("srcAzureConnectionString")
        self.source_container_name = os.getenv("sourceContainerName")
        self.target_container_name = os.getenv("targetMapperContainerName")
        self.source_blob_name = os.getenv("sourceFileName")
        self.boostrap_server = os.getenv("bootstrapServer")
        self.logger = Logging().create_logger()

        self.data_trans = DataTransection(
            azure_conn_str=self.azure_conn_str,
            source_container_name=self.source_container_name,
            target_container_name=self.target_container_name,
            source_blob_name=self.source_blob_name,
            target_blob_name=self.target_file_name,
        )

        self.kafka_trans = KafkaTransection(boostrap_server=self.boostrap_server)

    def read_records(self):
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        return data

    def write_records(self, data):
        self.data_trans.write_data(cloud_vendor=self.cloud_provider, data=data)

    def send_to_kafka(self):
        if self.process_type.lower() == "file":
            message = {
                "processType": "file",
                "fileName": self.target_file_name,
            }
            self.kafka_trans.send_message(
                target_topic=self.target_kafka_topic, message=message
            )
