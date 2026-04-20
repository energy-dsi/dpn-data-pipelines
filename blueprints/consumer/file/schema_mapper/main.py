import base64
import os
import time
from datetime import datetime

import schedule
from dotenv import load_dotenv

from utils.data_transection import DataTransection
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()


class SchemaMapper:
    def __init__(self):
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("targetTopicName")
        self.source_kafka_topic = os.getenv("mapperTopicName")
        self.source_azure_conn_str = base64.b64decode(
            os.getenv("mapperConnectionString")
        ).decode("utf-8")
        self.source_container_name = os.getenv("mapperContainerName")
        self.target_container_name = os.getenv("targetContainerName")
        self.boostrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = base64.b64decode(
            os.getenv("targetConnectionString")
        ).decode("utf-8")
        self.org_name = None
        self.schema_type = None
        self.file_name = None
        self.original_file_name = None

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

        self.logger.info("srcContainerName : %s", self.source_container_name)
        self.logger.info("mapperContainerName : %s", self.target_container_name)
        self.logger.info("bootstrapServer : %s", self.boostrap_server)

    def read_from_kafka_topic(self):
        """
        Read the message from the Kafka Topic

        Args:
            None

        Return:
            file_name (string): File name from the Kafka Topic
        """
        # file_name, container_name = self.kafka_trans.read_message(self, topic_name = source_kafka_topic)
        file_name = "eq-neso-eqbd.xml"
        self.data_trans.source_blob_name = file_name
        self.data_trans.target_blob_name = file_name

        file_props = file_name.split("-")

        self.schema_type = file_props[0]
        self.org_name = file_props[1]
        self.original_file_name = file_props[2]

        self.logger.info("file name properties: %s", file_props)

        return file_name

    def read_records(self) -> str:
        """
        Read the data from the source file from the source blob storage

        Args:
            None

        Return:
            None
        """
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        return data

    def schema_validation(self, data: str) -> bool:
        """
        Validate the data with it's respective schema

        Args:
            data (string): The data from the source file to validate

        Return:
            Boolean: If the data is valid, It will return `True`, Otherwise `False`
        """
        print("read the data from file")
        print(data)

        return True

    def move_files(self, file: str) -> None:
        """
        Move the file from source blob storage to target blob storage

        Args:
            file (string): file name to move

        Return:
            None
        """
        date_str = datetime.now().strftime("%d%m%Y")
        self.file_name = (
            self.schema_type.lower()
            + "-"
            + self.org_name.lower()
            + "-"
            + file.replace(" ", "_").replace("-", "_").lower()
        )
        self.file_name = (
            self.schema_type.lower()
            + "/"
            + self.org_name.lower()
            + "/"
            + date_str
            + "/"
            + self.file_name
        )

        self.data_trans.file_move(
            cloud_vendor=self.cloud_provider,
            file_name=file,
            dest_file_name=self.file_name,
        )

    def send_to_kafka(self) -> None:
        """
        Send a Kafka message to the mapper kafka topic

        Args:
            None

        Return:
            None
        """
        # Prepare a kafka message
        message = {
            "sourceType": self.cloud_provider,
            "storageContainer": self.target_container_name,
            "path": self.file_name,
        }
        print(f"Kafka message: {message}")
        # Sent a kafka message
        # self.kafka_trans.send_message(
        #     target_topic=self.target_kafka_topic, message=message
        # )


def main():
    """
    `main()` function to invoke the `AdaptorFileProcess` class

    Args:
        None

    Return:
        None
    """
    schema_mapper = SchemaMapper()
    file_name = schema_mapper.read_from_kafka_topic()
    if file_name is not None:
        data = schema_mapper.read_records()
        is_valid = schema_mapper.schema_validation(data)
        if is_valid == True:
            schema_mapper.move_files(file=file_name)
            schema_mapper.send_to_kafka()


# Scheduler to invoke the `main()` function in given interval
interval = int(os.getenv("scheduleInterval"))
schedule.every(interval).seconds.do(main)
while True:
    schedule.run_pending()
    time.sleep(1)
