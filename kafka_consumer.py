from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import os
import math
from datetime import datetime
import logging
from config import KAFKA_CONFIG, KAFKA_TOPIC

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('consumer.log'),
        logging.StreamHandler()
    ]
)

def create_consumer():
    config = KAFKA_CONFIG.copy()
    config.update({
        'group.id': 'amazon_sales_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000
    })
    logging.info("Creating Kafka consumer...")
    return Consumer(config)

def save_batch(batch_data, model_name):
    if not batch_data:
        return
        
    output_path = f'data/{model_name}.csv'
    os.makedirs('data', exist_ok=True)
    
    df = pd.DataFrame(batch_data)
    df.to_csv(output_path, index=False)
    logging.info(f"Saved {model_name} with {len(batch_data)} records")
    if not df.empty:
        logging.info(f"First product ID: {df['product_id'].iloc[0]}")
        logging.info(f"Last product ID: {df['product_id'].iloc[-1]}")

def save_batches_by_thirds(accumulated_data):
    total_count = len(accumulated_data)
    one_third_count = math.ceil(total_count / 3)
    
    logging.info(f"Processing data in thirds. Total records: {total_count}")
    logging.info(f"One third size: {one_third_count}")
    
    # Model 1: First 1/3 of data
    batch_1 = accumulated_data[:one_third_count]
    save_batch(batch_1, 'model_1')
    logging.info(f"Saved model_1 with {len(batch_1)} records")
    
    # Model 2: First 2/3 of data
    batch_2 = accumulated_data[:2 * one_third_count]
    save_batch(batch_2, 'model_2')
    logging.info(f"Saved model_2 with {len(batch_2)} records")
    
    # Model 3: All data
    save_batch(accumulated_data, 'model_3')
    logging.info(f"Saved model_3 with {len(accumulated_data)} records")

def process_messages():
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"Subscribed to topic: {KAFKA_TOPIC}")
    
    accumulated_data = []
    total_expected = 1466  # Total expected records
    start_time = datetime.now()
    
    logging.info(f"Started consuming messages at {start_time}")
    logging.info(f"Expecting {total_expected} records")
    
    try:
        while len(accumulated_data) < total_expected:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info("Reached end of partition")
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                accumulated_data.append(data)
                current_count = len(accumulated_data)
                
                # Log progress
                logging.info(f"Received record {current_count}/{total_expected} " +
                           f"({(current_count/total_expected)*100:.2f}%)")
                
                # Save intermediate batches at specific thresholds
                if current_count == math.ceil(total_expected/3):
                    logging.info("Reached 1/3 of data")
                    save_batch(accumulated_data, 'model_1')
                
                elif current_count == math.ceil(2 * total_expected/3):
                    logging.info("Reached 2/3 of data")
                    save_batch(accumulated_data[:current_count], 'model_2')
                
                elif current_count == total_expected:
                    logging.info("Reached full dataset")
                    save_batches_by_thirds(accumulated_data)
                    break
                
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue

    except KeyboardInterrupt:
        logging.info("Received interrupt signal, saving current progress...")
        if accumulated_data:
            save_batches_by_thirds(accumulated_data)
    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Consumer finished at {end_time}")
        logging.info(f"Total duration: {duration}")
        logging.info(f"Total records processed: {len(accumulated_data)}")
        consumer.close()

if __name__ == "__main__":
    process_messages()