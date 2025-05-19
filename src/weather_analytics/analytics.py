import json
import time
import uuid # For generating unique alert IDs
from datetime import datetime, timezone
from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
import logging
import os

# --- Logging Setup ---
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_file_path = os.path.join(LOG_DIR, 'analytics.log')
print(f"Analytics logs are being written to {log_file_path}") # User notification

# Configure logger
logger = logging.getLogger('weather_analytics')
logger.setLevel(logging.INFO)
# File handler
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.INFO)
# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
# --- End Logging Setup ---

# Kafka Configuration
BOOTSTRAP_SERVERS = 'localhost:29092'
INPUT_TOPIC = 'weather_data'
OUTPUT_TOPIC = 'weather_alerts'
CONSUMER_GROUP_ID = 'weather-analytics-group' # Unique group ID for this analytics consumer
AUTO_OFFSET_RESET = 'latest' # Process new messages as they arrive

# Extreme Weather Thresholds
EXTREME_TEMP_THRESHOLD_HOT = 30.0  # Celsius
EXTREME_WIND_THRESHOLD_HOT = 25.0  # km/h
EXTREME_TEMP_THRESHOLD_COLD = 0.0   # Celsius
EXTREME_WIND_THRESHOLD_COLD = 20.0 # km/h

def create_kafka_consumer(topic, bootstrap_servers, group_id, auto_offset_reset_config):
    """Creates a Kafka consumer."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset_config,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8')
        )
        logger.info(f"Analytics Consumer for topic '{topic}' created successfully.")
        return consumer
    except KafkaError as e:
        logger.error(f"Error creating Analytics Consumer: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error creating Analytics Consumer: {e}", exc_info=True)
        return None

def create_kafka_producer(bootstrap_servers):
    """Creates a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        logger.info("Analytics Producer created successfully.")
        return producer
    except KafkaError as e:
        logger.error(f"Error creating Analytics Producer: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error creating Analytics Producer: {e}", exc_info=True)
        return None

def process_message(message_value):
    """Processes a single weather data message and returns an alert message if applicable."""
    logger.debug(f"Processing message value: {message_value}")
    try:
        station_id = message_value.get('station_id')
        timestamp_str = message_value.get('timestamp')
        temperature = message_value.get('temperature')
        wind_speed = message_value.get('wind_speed')

        if None in [station_id, timestamp_str, temperature, wind_speed]:
            logger.warning(f"Skipping message due to missing fields:\n{json.dumps(message_value, indent=4)}")
            return None

        alert_type = None
        # Check for hot and windy
        if temperature > EXTREME_TEMP_THRESHOLD_HOT and wind_speed > EXTREME_WIND_THRESHOLD_HOT:
            alert_type = "Heat Wave with High Winds Alert"
        # Check for cold and windy (blizzard-like)
        elif temperature < EXTREME_TEMP_THRESHOLD_COLD and wind_speed > EXTREME_WIND_THRESHOLD_COLD:
            alert_type = "Blizzard Condition Alert"
        # Add more conditions here if needed (e.g., heavy rain, flooding)

        if alert_type:
            alert_id = f"alert_{str(uuid.uuid4())[:8]}_{station_id}" # Generate a unique alert ID
            alert_message = {
                "alert_id": alert_id,
                "station_id": station_id,
                "alert_type": alert_type,
                "triggering_timestamp": timestamp_str,
                "temperature": temperature,
                "wind_speed": wind_speed,
                "processed_at": datetime.now(timezone.utc).isoformat()
            }
            logger.info(f"Alert condition met. Generated alert:\n{json.dumps(alert_message, indent=4)}")
            return alert_message
        logger.debug("No alert conditions met for this message.")
        return None

    except Exception as e:
        logger.error(f"Error processing message value {json.dumps(message_value, indent=4)}: {e}", exc_info=True)
        return None

def on_send_success(record_metadata):
    logger.info(f"Alert sent to topic '{record_metadata.topic}', partition {record_metadata.partition}, offset {record_metadata.offset}")

def on_send_error(excp):
    logger.error(f"Error sending alert: {excp}")

if __name__ == "__main__":
    logger.info("--- Weather Analytics Started ---")
    consumer = create_kafka_consumer(INPUT_TOPIC, BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID, AUTO_OFFSET_RESET)
    producer = create_kafka_producer(BOOTSTRAP_SERVERS)

    if not consumer or not producer:
        logger.error("Failed to create consumer or producer. Exiting.")
        if consumer: consumer.close()
        if producer: producer.close()
        logger.info("--- Weather Analytics Finished (with error) ---")
        exit(1)

    logger.info(f"Starting weather analytics: Consuming from '{INPUT_TOPIC}', Producing to '{OUTPUT_TOPIC}'. Press Ctrl+C to stop.")
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
            logger.info(f"Received raw weather data:\n{json.dumps(log_msg_details, indent=4)}")
            alert_to_send = process_message(message.value)

            if alert_to_send:
                # Console print for immediate feedback during interactive run
                print(f"  >> Extreme condition detected for {alert_to_send['station_id']}: {alert_to_send['alert_type']}")
                print(f"  >> Producing alert: {json.dumps(alert_to_send, indent=2)}")
                logger.info(f"Producing alert:\n{json.dumps(alert_to_send, indent=4)}")
                producer.send(OUTPUT_TOPIC, key=alert_to_send['station_id'], value=alert_to_send).add_callback(on_send_success).add_errback(on_send_error)
            else:
                # Console print for immediate feedback
                print("  >> No extreme conditions detected for this message.")
                logger.debug("No extreme conditions detected for this message after process_message call.")

    except KeyboardInterrupt:
        logger.info("\nAnalytics interrupted by user (Ctrl+C).")
    except Exception as e:
        logger.error(f"An unexpected error occurred in the analytics main loop: {e}", exc_info=True)
    finally:
        logger.info("Shutting down analytics...")
        if consumer:
            logger.info("Closing analytics consumer...")
            consumer.close()
            logger.info("Analytics consumer closed.")
        if producer:
            logger.info("Flushing and closing analytics producer...")
            producer.flush()
            producer.close()
            logger.info("Analytics producer closed.")
        logger.info("Analytics shutdown complete.")
    logger.info("--- Weather Analytics Finished ---")
    print(f"Analytics logs have been saved to {log_file_path}") # User notification 