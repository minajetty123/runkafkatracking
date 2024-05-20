import os
from kafka import KafkaProducer
from kafka.errors import KafkaError


class RunKafka:

    def __init__(self, config):
        self.config = config
        run_path = os.path.dirname(__file__)
        self.producer = KafkaProducer(
            bootstrap_servers=self.config["kafka_bootstrap_servers"],
            security_protocol=self.config["kafka_security_protocol"],
            ssl_cafile=os.path.join(run_path, "kafkacerts/ca.pem"),
            ssl_certfile=os.path.join(run_path, "kafkacerts/service.cert"),
            ssl_keyfile=os.path.join(run_path, "kafkacerts/service.key")
        )

    def send_messages(self, topic_name, content):
        message = f"Hello from Python using SSL {content}!"
        future = self.producer.send(topic_name, content.encode('utf-8'))
        try:
            record_metadata = future.get(timeout=self.config["kafka_send_timeout_seconds"])
            print(f"Message sent: {message}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
        except KafkaError as e:
            print(f"Failed to send message: {e}")

    def close(self):
        self.producer.close()
