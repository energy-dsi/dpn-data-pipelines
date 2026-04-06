import os
from dotenv import load_dotenv

from utils.data_transection import DataTransection
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()

class EQSchemaMapper:
    def __init__(self):
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("dataProducerTopicName")
        self.target_file_name = os.getenv("dataProducerFileName")
        self.source_azure_conn_str = os.getenv("targetConnectionString").strip()
        self.source_container_name = os.getenv("targetContainerName")
        self.target_container_name = os.getenv("dataProducerContainerName")
        self.source_blob_name = os.getenv("targetMapperFileName")
        self.boostrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = os.getenv("dataProducerAzureConnectionString").strip()

        self.logger = Logging().create_logger()

        self.data_trans = DataTransection(
            source_azure_conn_str=self.source_azure_conn_str,
            source_container_name=self.source_container_name,
            target_container_name=self.target_container_name,
            source_blob_name=self.source_blob_name,
            target_blob_name=self.target_file_name,
            target_azure_conn_str = self.target_azure_conn_str,
        )

        self.kafka_trans = KafkaTransection(boostrap_server=self.boostrap_server)

    def read_records(self):
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        return data

    def write_records(self, data):
        self.data_trans.write_data(cloud_vendor=self.cloud_provider, data=data)

def main():
    eq_schema_validate = EQSchemaMapper()
    data = eq_schema_validate.read_records()
    eq_schema_validate.write_records(data=data)
    # eq_schema_validate.send_to_kafka()

if __name__ == "__main__":
    main()
