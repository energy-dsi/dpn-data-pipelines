import os
import schedule
import time

from dotenv import load_dotenv

from utils.data_transection import DataTransection
from utils.kafka_transection import KafkaTransection
from utils.logging import Logging

load_dotenv()


class AdaptorFileProcess:
    def __init__(self):
        self.cloud_provider = os.getenv("cloudProviderType")
        self.target_kafka_topic = os.getenv("mapperTopicName")
        self.source_azure_conn_str = os.getenv("srcConnectionString").strip()
        self.source_container_name = os.getenv("srcContainerName")
        self.target_container_name = os.getenv("mapperContainerName")
        self.boostrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = os.getenv("mapperConnectionString").strip()

        self.logger = Logging().create_logger()

        self.data_trans = DataTransection(
            source_azure_conn_str=self.source_azure_conn_str,
            source_container_name=self.source_container_name,
            target_container_name=self.target_container_name,
            source_blob_name=None,
            target_blob_name=None,
            target_azure_conn_str = self.target_azure_conn_str,
        )

        self.kafka_trans = KafkaTransection(boostrap_server=self.boostrap_server)

        print(f"self.cloud_provider: {self.cloud_provider}")
        print(f"self.source_azure_conn_str: {self.source_azure_conn_str}")

    def compare_file(self, source_file_info, target_file_info):
        file_name = self.data_trans.compare_file_and_ts(source_file_info = source_file_info, target_file_info = target_file_info)
        return file_name

    def read_file_info(self):
        source_file_info, target_file_info = self.data_trans.read_file_info(cloud_vendor = self.cloud_provider)
        return source_file_info, target_file_info

    def read_records(self, file_name):
        self.data_trans.source_blob_name = file_name
        self.data_trans.target_blob_name = file_name
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        return data

    def write_records(self, data):
        self.data_trans.write_data(cloud_vendor=self.cloud_provider, data=data)

    def send_to_kafka(self, file_name):
        message = {"sourceType": self.cloud_provider,
            "storageContainer": self.target_container_name,
            "path": file_name}

        self.kafka_trans.send_message(
            target_topic=self.target_kafka_topic, message=message
        )

def main():
    print("invoked")
    adaptor_file_process = AdaptorFileProcess()
    source_file_info, target_file_info = adaptor_file_process.read_file_info()
    process_files = adaptor_file_process.compare_file(source_file_info = source_file_info, target_file_info = target_file_info)
    for file in process_files:
        print("file")
        print(file)
        data = adaptor_file_process.read_records(file_name = file)
        adaptor_file_process.write_records(data=data)
        # adaptor_file_process.send_to_kafka(file_name = file)

interval = int(os.getenv("scheduleInterval"))
schedule.every(interval).seconds.do(main)
while True:
    schedule.run_pending()
    time.sleep(1)
