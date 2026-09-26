# import unittest
# from unittest.mock import ANY, MagicMock, Mock, patch

# from utils.kafka_transection import KafkaTransection


# class TestKafkaTransectin(unittest.TestCase):
#     @patch("utils.kafka_transection.KafkaProducer")
#     def test_send_message(self, mock_kafka_producer):
#         # Arrange
#         mock_producer_instance = Mock()
#         mock_kafka_producer.return_value = mock_producer_instance

#         kafka = KafkaTransection("localhost:9092")

#         message = {"key": "value"}

#         # Act
#         kafka.send_message("test-topic", message)

#         # Assert
#         mock_kafka_producer.assert_called_once()
#         mock_producer_instance.send.assert_called_once_with("test-topic", value=message)
#         mock_producer_instance.flush.assert_called_once()

#     @patch("utils.kafka_transection.KafkaConsumer")
#     def test_read_message(self, mock_kafka_consumer):
#         # Arrange
#         mock_consumer_instance = Mock()
#         mock_kafka_consumer.return_value = mock_consumer_instance

#         kafka = KafkaTransection("localhost:9092")
#         kafka.bootstrap_servers = "localhost:9092"  # required due to variable mismatch

#         # Act
#         consumer = kafka.read_message("test-topic")

#         # Assert
#         mock_kafka_consumer.assert_called_once_with(
#             "test-topic",
#             bootstrap_servers="localhost:9092",
#             auto_offset_reset="earliest",
#             enable_auto_commit=True,
#             value_deserializer=ANY,
#         )

#         assert consumer == mock_consumer_instance

#     def test_send_message_exception(self):
#         # Arrange
#         kafka = KafkaTransection("localhost:9092")
#         kafka.bootstrap_server = "localhost:9092"
#         kafka.logger = MagicMock()

#         target_topic = "test-topic"
#         message = {"key": "value"}

#         # Force KafkaProducer to raise an exception
#         with patch(
#             "utils.kafka_transection.KafkaProducer",
#             side_effect=Exception("Kafka error"),
#         ):

#             # Act
#             kafka.send_message(target_topic, message)

#             # Assert
#             kafka.logger.error.assert_called_once()
#             args, kwargs = kafka.logger.error.call_args

#             assert args[0] == "Kafka send failed"
#             assert "exc_info" in kwargs
#             assert isinstance(kwargs["exc_info"], Exception)
