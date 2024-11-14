from confluent_kafka import Producer
import pandas as pd
import time
import json
from datetime import datetime
import logging
from config import KAFKA_CONFIG, KAFKA_TOPIC, CSV_PATH

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('producer.log'),
        logging.StreamHandler()
    ]
)

def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def create_producer():
    logging.info("Creating Kafka producer...")
    producer_config = KAFKA_CONFIG.copy()
    producer_config.update({
        'linger.ms': 1,              # Minimal delay
        'batch.size': 1,             # Minimal valid batch size
        'compression.type': 'none',   # No compression for speed
        'queue.buffering.max.ms': 1,  # Minimal buffering
        'acks': 1                     # Wait for leader acknowledgment
    })
    return Producer(producer_config)

def clean_price(price):
    if pd.isna(price):
        return 0.0
    if isinstance(price, str):
        cleaned = price.replace('₹', '').replace(',', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return float(price)

def clean_percentage(percentage):
    if pd.isna(percentage):
        return 0.0
    if isinstance(percentage, str):
        cleaned = percentage.replace('%', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return float(percentage)

def clean_number(number):
    if pd.isna(number):
        return 0
    if isinstance(number, str):
        cleaned = number.replace(',', '').strip()
        try:
            return int(cleaned)
        except ValueError:
            return 0
    return int(number)

def safe_float_convert(value, default=0.0):
    if pd.isna(value):
        return default
    try:
        if isinstance(value, str):
            value = value.strip()
        if value == '' or str(value).lower() == 'nan':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def stream_data():
    producer = create_producer()
    start_time = datetime.now()
    
    logging.info(f"Starting data streaming at {start_time}")
    
    # Read Amazon Sales dataset
    logging.info(f"Reading data from {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    total_records = len(df)
    logging.info(f"Total records to process: {total_records}")
    
    # Clean and convert data
    logging.info("Cleaning and converting data...")
    df['discounted_price'] = df['discounted_price'].apply(clean_price)
    df['actual_price'] = df['actual_price'].apply(clean_price)
    df['discount_percentage'] = df['discount_percentage'].apply(clean_percentage)
    df['rating'] = df['rating'].apply(safe_float_convert)
    df['rating_count'] = df['rating_count'].apply(clean_number)

    try:
        for index, row in df.iterrows():
            data = {
                'product_id': str(row['product_id']),
                'product_name': str(row['product_name']),
                'category': str(row['category']),
                'discounted_price': float(row['discounted_price']),
                'actual_price': float(row['actual_price']),
                'discount_percentage': float(row['discount_percentage']),
                'rating': float(row['rating']),
                'rating_count': int(row['rating_count']),
                'about_product': str(row['about_product']),
                'user_id': str(row['user_id']),
                'user_name': str(row['user_name']),
                'review_id': str(row['review_id']),
                'review_title': str(row['review_title']),
                'review_content': str(row['review_content']),
                'img_link': str(row['img_link']),
                'product_link': str(row['product_link']),
                'timestamp': datetime.now().isoformat()
            }
            
            # Produce message
            producer.produce(
                KAFKA_TOPIC,
                key=str(row['product_id']),
                value=json.dumps(data),
                callback=delivery_report
            )
            
            # Poll and flush to ensure message is sent
            producer.poll(0)
            producer.flush()  # Make sure the message is sent before continuing
            
            if (index + 1) % 10 == 0:  # Log every 10 records
                logging.info(f"Progress: {index + 1}/{total_records} records processed ({((index + 1)/total_records)*100:.2f}%)")
            
            # Add delay to simulate real-time streaming
            time.sleep(0.5)  # 500ms delay between messages
            
    except Exception as e:
        logging.error(f"Error sending data: {e}")
        logging.error(f"Error at row: {index}")
        logging.error(f"Row data: {row}")
    finally:
        producer.flush()
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Streaming completed at {end_time}")
        logging.info(f"Total duration: {duration}")

if __name__ == "__main__":
    stream_data()