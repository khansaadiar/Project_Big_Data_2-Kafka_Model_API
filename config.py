# config.py
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'amazon-sales-client'
}
KAFKA_TOPIC = 'amazon_sales_topic'
BATCH_SIZE = 1000
MODEL_PATH = './models/'
CSV_PATH = 'amazon.csv'
