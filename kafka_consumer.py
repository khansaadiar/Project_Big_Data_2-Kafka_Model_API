#consumer.py
from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import os
import math
from config import KAFKA_CONFIG, KAFKA_TOPIC

def create_consumer():
    config = KAFKA_CONFIG.copy()
    config.update({
        'group.id': 'amazon_sales_group',
        'auto.offset.reset': 'earliest'
    })
    return Consumer(config)

def process_messages():
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    current_batch = []
    total_records = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                if current_batch:
                    save_all_batches(current_batch)
                    total_records += len(current_batch)
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if current_batch:
                        save_all_batches(current_batch)
                        total_records += len(current_batch)
                    break
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            try:
                data = json.loads(msg.value().decode('utf-8'))
                current_batch.append(data)

            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        if current_batch:
            save_all_batches(current_batch)
            total_records += len(current_batch)
        print(f"Final total records processed: {total_records}")
        consumer.close()

def save_all_batches(all_data):
    total_count = len(all_data)
    one_third_count = math.ceil(total_count / 3)

    # Model 1: First 1/3 of data
    batch_1 = all_data[:one_third_count]
    save_batch(batch_1, 'model_1')
    
    # Model 2: First 2/3 of data
    batch_2 = all_data[:2 * one_third_count]
    save_batch(batch_2, 'model_2')

    # Model 3: All data
    save_batch(all_data, 'model_3')

def save_batch(batch_data, model_name):
    df = pd.DataFrame(batch_data)
    output_path = f'data/{model_name}.csv'
    os.makedirs('data', exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved {model_name} with {len(batch_data)} records")
    print(f"First product ID in {model_name}: {df['product_id'].iloc[0]}")
    print(f"Last product ID in {model_name}: {df['product_id'].iloc[-1]}")

if __name__ == "__main__":
    process_messages()