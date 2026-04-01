import os

from dotenv import load_dotenv

from utils.data_transection import DataTransection
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()


class EQFileProcess:
    def __init__(self):
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("mapperTopicName")
        self.target_file_name = os.getenv("targetMapperFileName")
        self.source_azure_conn_str = os.getenv("adaptorAzureConnectionString").strip()
        self.source_container_name = os.getenv("adaptorContainerName")
        self.target_container_name = os.getenv("mapperContainerName")
        self.source_blob_name = os.getenv("sourceFileName")
        self.boostrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = os.getenv("mapperAzureConnectionString").strip()

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

        print(f"self.cloud_provider: {self.cloud_provider}")
        print(f"self.source_azure_conn_str: {self.source_azure_conn_str}")

    def read_records(self):
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        return data

    def write_records(self, data):
        self.data_trans.write_data(cloud_vendor=self.cloud_provider, data=data)

    def send_to_kafka(self):
        message = {
            "processType": "file",
            "fileName": self.target_file_name,
        }
        self.kafka_trans.send_message(
            target_topic=self.target_kafka_topic, message=message
        )

def main():
    eq_file_process = EQFileProcess()
    data = eq_file_process.read_records()
    eq_file_process.write_records(data=data)
    # eq_file_process.send_to_kafka()

if __name__ == "__main__":
    main()
