import json
import requests
import re
import time
from runkafka.RunKafka import RunKafka

with open('config.json') as f:
    config = json.load(f)
KAFKA_BOOTSTRAP_SERVERS = config["kafka_bootstrap_servers"]
REGEX_PATTERN = config["regex_pattern"]
KAFKA_TOPIC = config["kafka_topic"]
WEBSITES = config["websites"]
REQUEST_INTERVAL = config["website_requests_interval_seconds"]
run_kafka_producer = RunKafka(config)


def assess_website(url):
    try:
        start_time = time.time()
        response = requests.get(url)
        end_time = time.time()
        total_response_time = end_time - start_time

        if response.status_code == 200:
            if REGEX_PATTERN:
                if re.search(REGEX_PATTERN, response.text):
                    matched_response_body = response.text
                else:
                    matched_response_body = ""
            else:
                matched_response_body = None

            request_result = {
                "request_url": url,
                "status_code": response.status_code,
                "total_response_time": total_response_time,
                "matched_response_body": matched_response_body
            }
            request_result_json = json.dumps(request_result)
            print(f"Sending assess website result to Kafka: {request_result_json}")
            run_kafka_producer.send_messages(KAFKA_TOPIC, request_result_json)

    except Exception as e:
        # depend on situation, some broken messages might be send to dead letter queue in kafka
        print(f"requesting target website {url} occurred exception: {str(e)}")
        run_kafka_producer.close()


def main():
    while True:
        for website in WEBSITES:
            assess_website(website)
        time.sleep(REQUEST_INTERVAL)


if __name__ == "__main__":
    main()
