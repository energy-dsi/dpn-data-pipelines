import os
import base64
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
        self.source_azure_conn_str = base64.b64decode(os.getenv("mapperConnectionString")).decode("utf-8")
        self.source_container_name = os.getenv("mapperContainerName")
        self.target_container_name = os.getenv("targetContainerName")
        self.boostrap_server = os.getenv("bootstrapServer")
        self.target_azure_conn_str = os.getenv("targetConnectionString").strip()

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

        self.logger.info("cloudProviderType : %s ", self.cloud_provider)
        self.logger.info("mapperTopicName : %s", self.target_kafka_topic)
        self.logger.info("srcConnectionString : %s",self.source_azure_conn_str)
        self.logger.info("srcContainerName : %s",self.source_container_name)
        self.logger.info("mapperContainerName : %s",self.target_container_name)
        self.logger.info("bootstrapServer : %s",self.boostrap_server)
        self.logger.info("mapperConnectionString : %s",self.target_azure_conn_str)

    def read_from_kafka_topic(self):
        # file_name, container_name = self.kafka_trans.read_message(self, topic_name = source_kafka_topic)
        file_name = "eq.xml"
        self.data_trans.source_blob_name = file_name
        self.data_trans.target_blob_name = file_name

    def compare_file_and_ts(self, source_file_info, target_file_info):
        common_keys = source_file_info.keys() & target_file_info.keys()
        new_file = source_file_info.keys() - target_file_info.keys()
        file_to_process = list()
        for key in common_keys:
            if source_file_info[key] > target_file_info[key]:
                file_to_process.append(key)

        file_to_process.extend(new_file)
        return file_to_process

    def read_file_info(self):
        source_file_info, target_file_info = self.data_trans.read_file_info(cloud_vendor = self.cloud_provider)
        return source_file_info, target_file_info

    def read_records(self):
        data = self.data_trans.data_read(cloud_vendor=self.cloud_provider)
        return data

    def write_records(self, data):
        self.data_trans.write_data(cloud_vendor=self.cloud_provider, data=data)

def main():
    schema_mapper = SchemaMapper()
    schema_mapper.read_from_kafka_topic()
    source_file_info, target_file_info = schema_mapper.read_file_info()
    process_files = schema_mapper.compare_file_and_ts(source_file_info = source_file_info, target_file_info = target_file_info)
    data = schema_mapper.read_records()
    schema_mapper.write_records(data=data)
    # schema_mapper.send_to_kafka()

if __name__ == "__main__":
    main()
