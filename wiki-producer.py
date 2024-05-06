import json
import logging
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import socket

# Set up logging
logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

# Create producer
producer = KafkaProducer(
    bootstrap_servers=['b-2.532cluster3.946d6e.c3.kafka.us-east-1.amazonaws.com:9092','b-2.532cluster2.aqywu4.c3.kafka.us-east-1.amazonaws.com:9092','b-3.532cluster2.aqywu4.c3.kafka.us-east-1.amazonaws.com:9092'],
    #bootstrap_servers=['b-2.532cluster3.946d6e.c3.kafka.us-east-1.amazonaws.com:9092','b-3.532cluster3.946d6e.c3.kafka.us-east-1.amazonaws.com:9092','b-1.532cluster3.946d6e.c3.kafka.us-east-1.amazonaws.com:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol='PLAINTEXT',
    api_version=(2,6,0)
)
# Read streaming event
url = 'https://stream.wikimedia.org/v2/stream/recentchange'
try:
    for event in EventSource(url):
        if event.event == 'message':
            try:
                change = json.loads(event.data)
            except ValueError:
                continue  # Skip processing if the data cannot be loaded into JSON
            else:
                # Send msg to topic wiki-changes
                if 'en.wikipedia.org' in change['meta']['domain']:
                    # print('{user} edited {title}'.format(**change))
                    producer.send('wiki-changes', change).add_callback(on_send_success).add_errback(on_send_error)
except KeyboardInterrupt:
    print("Process interrupted")
finally:
    producer.flush()  # Ensure all messages are sent before closing
    producer.close()  # Close the producer to free up resources