import json
import time
import random
from datetime import datetime, timezone
from kafka.producer import KafkaProducer
from kafka.errors import KafkaError
import logging
import os

# --- Logging Setup ---
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_file_path = os.path.join(LOG_DIR, 'producer.log')
print(f"Producer logs are being written to {log_file_path}") # User notification

# Configure logger
logger = logging.getLogger('weather_producer')
logger.setLevel(logging.INFO)
# File handler
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.INFO)
# Console handler (optional, for also printing to console)
# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# ch.setFormatter(formatter)
logger.addHandler(fh)
# logger.addHandler(ch)
# --- End Logging Setup ---

# Kafka broker address
BOOTSTRAP_SERVERS = 'localhost:29092'

# Kafka topic to produce messages to
WEATHER_TOPIC = 'weather_data'

# Number of messages to send per station in this run, then exit
# Set to 0 or a negative number to run indefinitely until Ctrl+C
MESSAGES_PER_STATION_LIMIT = 5 

def create_producer():
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'), # Added key_serializer
            request_timeout_ms=60000, # Increased timeout to 60 seconds
            # api_version_auto_timeout_ms=30000 # Removed for now
        )
        logger.info("Kafka Producer created successfully.")
        return producer
    except KafkaError as e:
        logger.error(f"Error creating Kafka Producer: {e}")
        return None

def generate_weather_data(station_id: str) -> dict:
    """Generates a sample weather data point."""
    data = {
        "station_id": station_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(-5.0, 35.0), 1),  # Celsius
        "humidity": round(random.uniform(20.0, 100.0), 1),   # Percentage
        "wind_speed": round(random.uniform(0.0, 75.0), 1)    # km/h
    }
    logger.info(f"Generated data for {station_id}:\n{json.dumps(data, indent=4)}") # Beautified JSON log
    return data

def on_send_success(record_metadata):
    """Callback for successful message sending."""
    logger.info(f"Message sent to topic '{record_metadata.topic}', partition {record_metadata.partition}, offset {record_metadata.offset}")

def on_send_error(excp):
    """Callback for failed message sending."""
    logger.error(f"Error sending message: {excp}")

if __name__ == "__main__":
    logger.info("--- Weather Producer Started ---")
    producer = create_producer()

    if producer:
        station_ids = ["station_alpha", "station_bravo", "station_charlie"]
        messages_sent_count = {station_id: 0 for station_id in station_ids}
        
        logger.info("Producer instance created. Waiting 15 seconds for broker to initialize fully...")
        time.sleep(15) # Wait for 15 seconds

        logger.info(f"Starting to produce messages to topic: {WEATHER_TOPIC} on {BOOTSTRAP_SERVERS}")
        logger.info(f"Will send {MESSAGES_PER_STATION_LIMIT} messages per station (if > 0), then exit. Press Ctrl+C to stop early.")

        try:
            while True:
                all_stations_reached_limit = True
                for station_id in station_ids:
                    if MESSAGES_PER_STATION_LIMIT > 0 and messages_sent_count[station_id] >= MESSAGES_PER_STATION_LIMIT:
                        continue # Skip this station if its limit is reached
                    all_stations_reached_limit = False # At least one station is still sending

                    weather_data = generate_weather_data(station_id)
                    # logger.info(f"Sending to Kafka: {weather_data}") # Already logged in generate_weather_data
                    
                    future = producer.send(
                        WEATHER_TOPIC,
                        key=station_id, # Key will be serialized by key_serializer
                        value=weather_data
                    )
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                    
                    messages_sent_count[station_id] += 1

                if MESSAGES_PER_STATION_LIMIT > 0 and all_stations_reached_limit:
                    logger.info(f"All stations reached the message limit of {MESSAGES_PER_STATION_LIMIT}. Exiting loop.")
                    break 
                
                time.sleep(random.uniform(1, 3)) # Send data from each station at a slightly varied interval

        except KeyboardInterrupt:
            logger.info("\nGracefully shutting down producer due to Ctrl+C...")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        finally:
            if producer:
                logger.info("Flushing remaining messages...")
                producer.flush() # Ensure all buffered messages are sent
                logger.info("Closing producer.")
                producer.close()
                logger.info("Producer closed.")
        logger.info("--- Weather Producer Finished ---")
        print(f"Producer logs have been saved to {log_file_path}") # User notification
    else:
        logger.error("Producer could not be created. Exiting.")
        logger.info("--- Weather Producer Finished (with error) ---")
        print(f"Producer logs have been saved to {log_file_path}") # User notification 