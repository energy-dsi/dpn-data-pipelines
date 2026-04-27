import base64
import json
import os

from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError
from kafka import KafkaConsumer

from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()


class SchemaMapper:
    """
    `SchemaMapper` class is used to validate the data using validation schema
    """

    def __init__(self):
        """
        Initializes the `SchemaMapper` instance
        """
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("targetTopicName")
        self.source_kafka_topic = os.getenv("mapperTopicName")
        self.source_azure_conn_str = base64.b64decode(
            os.getenv("mapperConnectionString")
        ).decode("utf-8")
        self.source_container_name = os.getenv("mapperContainerName")
        self.target_container_name = os.getenv("targetContainerName")
        self.bootstrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = base64.b64decode(
            os.getenv("targetConnectionString")
        ).decode("utf-8")
        self.org_name = os.getenv("orgName")
        self.schema_type = os.getenv("schemaType")
        self.file_name = None

        # logger object creation
        self.logger = Logging().create_logger()

        # Object creation `DataTransection` class
        self.data_trans = DataTransection(
            source_azure_conn_str=self.source_azure_conn_str,
            source_container_name=self.source_container_name,
            target_container_name=self.target_container_name,
            source_blob_name=None,
            target_blob_name=None,
            target_azure_conn_str=self.target_azure_conn_str,
        )

        # Object creation of `KafkaTransection` class
        self.kafka_trans = KafkaTransection(bootstrap_server=self.bootstrap_server)

        log_lines = [
            "------------- Producer - Schema Mapper Config Information -------------",
            f"cloudProviderType    : {self.cloud_provider}",
            f"targetTopicName      : {self.target_kafka_topic}",
            f"srcContainerName     : {self.source_container_name}",
            f"targetContainerName  : {self.target_container_name}",
            f"bootstrapServer      : {self.bootstrap_server}",
        ]

        width = max(len(line) for line in log_lines) + 4
        border = "+" + "-" * (width - 2) + "+"

        self.logger.info(border)
        for line in log_lines:
            self.logger.info(f"| {line.ljust(width - 4)} |")
        self.logger.info(border)

        self.logger.info("Kafak listening at: %s", self.source_kafka_topic)

    def read_records(self, file) -> str:
        """
        Read the data from the source file from the source blob storage

        Args:
            None

        Return:
            None
        """
        log_lines = [
            "------------- Producer - Schema Mapper Config Information -------------",
            f"cloudProviderType    : {self.cloud_provider}",
            f"targetTopicName      : {self.target_kafka_topic}",
            f"srcContainerName     : {self.source_container_name}",
            f"targetContainerName  : {self.target_container_name}",
            f"bootstrapServer      : {self.bootstrap_server}",
        ]

        width = max(len(line) for line in log_lines) + 4
        border = "+" + "-" * (width - 2) + "+"

        self.logger.info(border)
        for line in log_lines:
            self.logger.info(f"| {line.ljust(width - 4)} |")
        self.logger.info(border)

        self.data_trans.source_blob_name = file
        # self.data_trans.target_blob_name=file
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

        # LOGIC NEEDS TO IMPLEMENT IN PI3
        self.logger.info("LOGIC NEEDS TO IMPLEMENT IN PI3")

        return True

    def move_files(self, file: str) -> None:
        """
        Move the file from source blob storage to target blob storage

        Args:
            file (string): file name to move

        Return:
            None
        """

        # File name preparation `<schematype>-<orgname>-<actual file name>`. e.g. eq-orga-abcd.xml
        self.file_name = (
            self.schema_type.lower()
            + "-"
            + self.org_name.lower()
            + "-"
            + file.replace(" ", "_").replace("-", "_").lower()
        )

        # Call the `file_move` method
        is_file_move = self.data_trans.file_move(
            cloud_vendor=self.cloud_provider,
            file_name=file,
            dest_file_name=self.file_name,
        )

        return is_file_move

    def send_to_kafka(self, is_file_move: bool) -> None:
        """
        Send a Kafka message to the mapper kafka topic

        Args:
            None

        Return:
            None
        """
        if is_file_move:
            # Prepare a kafka message
            message = {
                "sourceType": self.cloud_provider,
                "storageContainer": self.target_container_name,
                "path": self.file_name,
            }
            # Sent a kafka message
            self.kafka_trans.send_message(
                target_topic=self.target_kafka_topic, message=message
            )
            self.logger.info(
                "Message pushed into %s kafka topic", self.target_kafka_topic
            )
        else:
            self.logger.info("Message not sent to Kafka: file movement failed")


def main(schema_mapper, file):
    """
    `main()` function to invoke the `AdaptorFileProcess` class

    Args:
        file (string): File name from the source blob storage

    Return:
        None
    """
    message = "Schema Mapper Started"
    border = "=" * (len(message) + 4)

    print(border)
    print(f"| {message} |")
    print(border)

    except_handle = HandleExceptions()
    try:
        data = schema_mapper.read_records(file=file)
        is_valid = schema_mapper.schema_validation(data)
        if is_valid:
            is_file_move = schema_mapper.move_files(file=file)
            schema_mapper.send_to_kafka(is_file_move=is_file_move)

    except (HttpResponseError, AzureError) as e:
        except_handle.handle_storage_exception(e, "Azure")
    except (ClientError, BotoCoreError) as e:
        except_handle.handle_storage_exception(e, "AWS S3")
    except GoogleAPIError as e:
        except_handle.handle_storage_exception(e, "GCP")
    except Exception as e:
        # Anything else (bugs, invalid args, unexpected errors)
        except_handle.handle_storage_exception(e, "")

    message = "Schema Mapper Completed"
    border = "=" * (len(message) + 4)

    print(border)
    print(f"| {message} |")
    print(border)


def start_consumer(schema_mapper):

    topic = os.getenv("mapperTopicName")
    bootstrap_server = os.getenv("bootstrapServer")

    # Read the message from Kafka Topic
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="producer_schema_mapper",
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    for message in consumer:
        data = json.loads(message.value)
        file_name = data["path"]
        main(schema_mapper=schema_mapper, file=file_name)


if __name__ == "__main__":  # pragma: no cover
    # Code execution starting here
    schema_mapper = SchemaMapper()
    start_consumer(schema_mapper=schema_mapper)
