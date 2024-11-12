# Project_Big_Data_2-Kafka_Model_API

# Start Zookeeper
brew services start zookeeper

# Start Kafka
brew services start kafka

source env/bin/activate

# Create Kafka topic
kafka-topics --create --topic amazon_sales_topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

#delet
kafka-topics --delete --topic amazon_sales_topic --bootstrap-server localhost:9092

# Verify topic creation
kafka-topics --list --bootstrap-server localhost:9092


Root endpoint: http://127.0.0.1:5000/

Product details: http://127.0.0.1:5000/api/product/<product_id>

Similar products: http://127.0.0.1:5000/api/recommend/similar?product_id=<product_id>&n=5

Price-based recommendations: http://127.0.0.1:5000/api/recommend/price?price=<target_price>&n=5