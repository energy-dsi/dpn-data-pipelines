import base64
import os
import time

import schedule
from azure.core.exceptions import AzureError, HttpResponseError
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google.api_core.exceptions import GoogleAPIError

from utils.data_transection import DataTransection
from utils.exception_handler import HandleExceptions
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()


class AdaptorFileProcess:
    """
    File process class for Adaptor
    """

    def __init__(self):
        """
        Initializes the AdaptorFileProcess instance
        """
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("mapperTopicName")
        self.source_azure_conn_str = base64.b64decode(
            os.getenv("srcConnectionString")
        ).decode("utf-8")
        self.source_container_name = os.getenv("srcContainerName")
        self.target_container_name = os.getenv("mapperContainerName")
        self.bootstrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = base64.b64decode(
            os.getenv("mapperConnectionString")
        ).decode("utf-8")

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

        lines = [
            "------------- Producer - Adaptor Config Information -------------",
            f"cloudProviderType   : {self.cloud_provider}",
            f"mapperTopicName     : {self.target_kafka_topic}",
            f"srcContainerName    : {self.source_container_name}",
            f"mapperContainerName : {self.target_container_name}",
            f"bootstrapServer     : {self.bootstrap_server}",
        ]

        box_width = max(len(line) for line in lines) + 4
        border = "+" + "-" * (box_width - 2) + "+"

        self.logger.info(border)
        for line in lines:
            self.logger.info(f"| {line.ljust(box_width - 4)} |")
        self.logger.info(border)

    def read_source_file_info(self) -> list[str]:
        """
        Read the file name from the source blob storage

        Args:
            None

        Return:
            list: List of file name from the blob storage
        """

        source_file_name = self.data_trans.source_file_info(
            cloud_provider=self.cloud_provider
        )
        self.logger.info("File info: %s", source_file_name)

        return source_file_name

    def move_files(self, file: str) -> None:
        """
        Move the file from source blob storage to target blob storage

        Args:
            file (string): file name to move

        Return:
            None
        """
        is_file_moved = self.data_trans.file_move(
            cloud_vendor=self.cloud_provider, file_name=file
        )

        return is_file_moved

    def send_to_kafka(self, file_name: str, is_file_move: bool) -> None:
        """
        Send a Kafka message to the mapper kafka topic

        Args:
            file_name (string): The file moved to the mapper container

        Return:
            None
        """

        # Prepare a kafka message

        if is_file_move:
            message = {
                "sourceType": self.cloud_provider,
                "storageContainer": self.target_container_name,
                "path": file_name,
            }

            # Sent a kafka message
            self.kafka_trans.send_message(
                target_topic=self.target_kafka_topic, message=message
            )
            self.logger.info(
                "Message pushed into `%s` kafka topic", self.target_kafka_topic
            )
        else:
            self.logger.info("Message not sent to Kafka: file movement failed")


def main():
    """
    `main()` function to invoke the `AdaptorFileProcess` class

    Args:
        None

    Return:
        None
    """
    message = "Adaptor File Processor started"
    border = "=" * (len(message) + 4)

    print(border)
    print(f"| {message} |")
    print(border)

    except_handle = HandleExceptions()
    try:
        adaptor_file_process = AdaptorFileProcess()
        source_files = adaptor_file_process.read_source_file_info()
        for file in source_files:
            is_file_move = adaptor_file_process.move_files(file=file)
            adaptor_file_process.send_to_kafka(
                file_name=file, is_file_move=is_file_move
            )

    except (HttpResponseError, AzureError) as e:
        except_handle.handle_storage_exception(e, "Azure")
    except (ClientError, BotoCoreError) as e:
        except_handle.handle_storage_exception(e, "AWS S3")
    except GoogleAPIError as e:
        except_handle.handle_storage_exception(e, "GCP")
    except Exception as e:
        # Anything else (bugs, invalid args, unexpected errors)
        except_handle.handle_storage_exception(e, "")

    message = "Adaptor Process Completed"
    border = "=" * (len(message) + 4)

    print(border)
    print(f"| {message} |")
    print(border)


if __name__ == "__main__":  # pragma: no cover
    # Scheduler to invoke the `main()` function in given interval
    print("Adaptor Process Starting Based on Schedule Interval")
    interval = int(os.getenv("scheduleInterval"))
    schedule.every(interval).seconds.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
