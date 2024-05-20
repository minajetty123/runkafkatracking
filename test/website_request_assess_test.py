import unittest
import json

from runkafka.RunKafka import RunKafka


class TestWebsiteRequestAssess(unittest.TestCase):

    def test_run_kafka_produce_message(self):
        with open('../config.json') as f:
            config = json.load(f)
        KAFKA_TOPIC = config["kafka_topic"]

        try:
            kafka_ssl_producer = RunKafka(config)
            kafka_ssl_producer.send_messages(KAFKA_TOPIC, "test_only")
            kafka_ssl_producer.close()
        except Exception as e:
            self.fail(f"An exception occurred in assertRaises in test_avien_kafka_produce_message test case: {e}")