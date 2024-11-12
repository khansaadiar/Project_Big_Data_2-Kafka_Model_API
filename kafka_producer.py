# producer.py
from confluent_kafka import Producer
import pandas as pd
import time
import json
from config import KAFKA_CONFIG, KAFKA_TOPIC, CSV_PATH

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def create_producer():
    return Producer(KAFKA_CONFIG)

def clean_price(price):
    if pd.isna(price):
        return 0.0
    if isinstance(price, str):
        # Remove '₹' symbol and commas, then convert to float
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
        # Remove '%' symbol and convert to float
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
        # Remove any commas and convert to int
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
    
    # Read Amazon Sales dataset
    df = pd.read_csv(CSV_PATH)
    
    # Clean and convert data
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
                'product_link': str(row['product_link'])
            }
            
            producer.produce(
                KAFKA_TOPIC,
                key=str(row['product_id']),
                value=json.dumps(data),
                callback=delivery_report
            )
            producer.poll(0)
            print(f"Sent record {index + 1}")
            time.sleep(0.1)
            
    except Exception as e:
        print(f"Error sending data: {e}")
        print(f"Error at row: {index}")
        print(f"Row data: {row}")
    finally:
        producer.flush()

if __name__ == "__main__":
    stream_data()
    