import json
import logging
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer

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
    bootstrap_servers='localhost:9092', # Make sure this matches your Kafka server setup
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
