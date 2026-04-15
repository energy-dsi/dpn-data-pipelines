import base64
import os
import time

import schedule
from dotenv import load_dotenv

from utils.data_transection import DataTransection
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()


class ExtractorFileProcess:
    def __init__(self):
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("mapperTopicName")
        self.source_azure_conn_str = base64.b64decode(
            os.getenv("srcConnectionString")
        ).decode("utf-8")
        self.source_container_name = os.getenv("srcContainerName")
        self.target_container_name = os.getenv("mapperContainerName")
        self.boostrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = base64.b64decode(
            os.getenv("mapperConnectionString")
        ).decode("utf-8")

        self.logger = Logging().create_logger()

        self.data_trans = DataTransection(
            source_azure_conn_str=self.source_azure_conn_str,
            source_container_name=self.source_container_name,
            target_container_name=self.target_container_name,
            source_blob_name=None,
            target_blob_name=None,
            target_azure_conn_str=self.target_azure_conn_str,
        )

        self.kafka_trans = KafkaTransection(boostrap_server=self.boostrap_server)

        self.logger.info("cloudProviderType : %s ", self.cloud_provider)
        self.logger.info("mapperTopicName : %s", self.target_kafka_topic)
        self.logger.info("srcConnectionString : %s", self.source_azure_conn_str)
        self.logger.info("srcContainerName : %s", self.source_container_name)
        self.logger.info("mapperContainerName : %s", self.target_container_name)
        self.logger.info("bootstrapServer : %s", self.boostrap_server)
        self.logger.info("mapperConnectionString : %s", self.target_azure_conn_str)

    def read_source_file_info(self) -> list[str]:
        """
        Read the file name from the source blob storage
        
        Args:
            None

        Return:
            list: List of file name from the blob storage
        """
        source_file_name = self.data_trans.source_file_info(cloud_provider = self.cloud_provider)
        return source_file_name

    def move_files(self, file: str) -> None:
        """
        Move the file from source blob storage to target blob storage

        Args:
            file (string): file name to move

        Return:
            None 
        """
        self.data_trans.file_move(cloud_vendor = self.cloud_provider, file_name = file)

    def send_to_kafka(self, file_name: str) -> None:
        """
        Send a Kafka message to the mapper kafka topic
        
        Args:
            file_name (string): The file moved to the mapper container

        Return:
            None 
        """
        # Prepare a kafka message
        message = {
            "sourceType": self.cloud_provider,
            "storageContainer": self.target_container_name,
            "path": file_name,
        }

        self.logger.info("Kafka message: %s", message)

        # Sent a kafka message
        # self.kafka_trans.send_message(
        #     target_topic=self.target_kafka_topic, message=message
        # )


def main():
    """
    `main()` function to invoke the `ExtractorFileProcess` class

    Args: 
        None

    Return:
        None
    """
    print("invoked")
    extractor_file_process = ExtractorFileProcess()
    source_files = extractor_file_process.read_source_file_info()
    for file in source_files:
        print(f"======================{file}=====================")
        extractor_file_process.move_files(file = file)
        extractor_file_process.send_to_kafka(file_name = file)

# Scheduler to invoke the `main()` function in given interval 
interval = int(os.getenv("scheduleInterval"))
schedule.every(interval).seconds.do(main)
while True:
    schedule.run_pending()
    time.sleep(1)
