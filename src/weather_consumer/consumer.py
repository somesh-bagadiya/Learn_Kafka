import json
from kafka.consumer import KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime, timezone
import logging
import os

# --- Logging Setup ---
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_file_path = os.path.join(LOG_DIR, 'consumer.log')
print(f"Consumer logs are being written to {log_file_path}") # User notification

# Configure logger
logger = logging.getLogger('weather_consumer')
logger.setLevel(logging.INFO)
# File handler
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.INFO)
# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
# --- End Logging Setup ---

# Kafka broker address - should match producer
BOOTSTRAP_SERVERS = 'localhost:29092'

# Kafka topic to consume messages from - should match producer
WEATHER_TOPIC = 'weather_data'

# Consumer group ID
# All consumers with the same group ID will be part of the same consumer group.
# Kafka ensures that each message in a subscribed topic is delivered to only one consumer instance within its group.
CONSUMER_GROUP_ID = 'weather-report-group-1'

# How the consumer behaves when it starts and there's no existing offset for its group, 
# or if the current offset is out of range.
# 'earliest': automatically reset the offset to the earliest offset
# 'latest': automatically reset the offset to the latest offset (default)
# 'none': throw exception to the consumer if no previous offset is found
AUTO_OFFSET_RESET = 'earliest' # Start reading from the beginning of the topic if no offset is stored

def create_consumer(topic_name, bootstrap_servers, group_id, auto_offset_reset_config):
    """Creates and returns a KafkaConsumer instance."""
    logger.info(f"Attempting to subscribe to topic: {topic_name} as group: {group_id}")
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset_config,
            # Decode UTF-8 bytes to string, then parse JSON string to dict for the value
            value_deserializer=lambda v_bytes: json.loads(v_bytes.decode('utf-8')),
            # Decode UTF-8 bytes to string for the key
            key_deserializer=lambda k_bytes: k_bytes.decode('utf-8')
        )
        logger.info("Kafka Consumer created successfully.")
        return consumer
    except KafkaError as e:
        logger.error(f"Error creating Kafka Consumer: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while creating consumer: {e}", exc_info=True)
        return None

def consume_messages(consumer):
    """Continuously polls for messages and processes them."""
    if not consumer:
        logger.warning("Consumer instance is None. Cannot consume messages.")
        return

    logger.info("Waiting for messages... (Press Ctrl+C to stop)")
    try:
        for message in consumer:
            log_msg_details = {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp_ms": message.timestamp,
                "datetime_utc": datetime.fromtimestamp(message.timestamp / 1000, tz=timezone.utc).isoformat(),
                "key": message.key,
                "value": message.value
            }
            logger.info(f"Received message:\n{json.dumps(log_msg_details, indent=4)}") # Beautified JSON log
            
            # Example: Simple processing - access specific fields from the weather data
            station_id = message.value.get('station_id')
            temperature = message.value.get('temperature')
            humidity = message.value.get('humidity') # Added humidity for completeness

            if station_id and temperature is not None and humidity is not None:
                processed_output = f"Processed Weather Update: Station {station_id} - Temp: {temperature}°C, Humidity: {humidity}%"
                logger.info(processed_output)
                print(f"  >> {processed_output}") # Keep console print for interactive view
            else:
                logger.warning(f"Message value does not contain expected fields (station_id, temperature, humidity) or is not a dict. Value:\n{json.dumps(message.value, indent=4)}") # Beautified JSON log
                print("  >> Note: Message value is a dict, but expected fields missing or not a dict.")

    except KeyboardInterrupt:
        logger.info("\nConsumer interrupted by user (Ctrl+C).")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from message value: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred while consuming messages: {e}", exc_info=True)
    finally:
        logger.info("Closing Kafka Consumer...")
        if consumer:
            consumer.close()
        logger.info("Kafka Consumer closed.")

if __name__ == "__main__":
    # Ensure timezone is available for the datetime conversion in consume_messages
    from datetime import timezone 
    logger.info("--- Weather Consumer Started ---")
    
    consumer_instance = create_consumer(
        WEATHER_TOPIC, 
        BOOTSTRAP_SERVERS, 
        CONSUMER_GROUP_ID, 
        AUTO_OFFSET_RESET
    )
    
    if consumer_instance:
        consume_messages(consumer_instance)
    else:
        logger.error("Could not create consumer instance. Exiting.")
    logger.info("--- Weather Consumer Finished ---")
    print(f"Consumer logs have been saved to {log_file_path}") # User notification 