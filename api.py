from flask import Flask, jsonify, request
import os
import json
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType

os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home'

# Inisialisasi Spark session
spark = SparkSession.builder \
    .appName("Big Data API") \
    .getOrCreate()

# Define constants
DATASET_PATHS = {
    'small': 'data/model_1.csv',
    'medium': 'data/model_2.csv',
    'full': 'data/model_3.csv'
}
K = 3  # Number of clusters

# Flask app
app = Flask(__name__)

@app.route('/')
def home():
    """
    Simple home endpoint to verify the API is running.
    """
    return jsonify({"message": "Big Data API is running."})

def preprocess_and_cluster(data, features):
    """
    Preprocess data and apply KMeans clustering.
    """
    # Convert numeric values to float
    for row in data:
        for key in row:
            if isinstance(row[key], int):  # Check if value is an integer
                row[key] = float(row[key])

    schema = StructType([StructField(col, DoubleType(), True) for col in features])
    df = spark.createDataFrame(data, schema)

    # Assemble features into a single vector
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    df_vectorized = assembler.transform(df)

    # Scale the features
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_vectorized)
    df_scaled = scaler_model.transform(df_vectorized)

    # Apply KMeans clustering
    kmeans = KMeans(k=3, seed=42, featuresCol="scaled_features")
    model = kmeans.fit(df_scaled)
    clustered_df = model.transform(df_scaled)

    # Collect results
    result = clustered_df.select(*features, "prediction").collect()
    return [{"features": row.asDict(), "cluster": row["prediction"]} for row in result]


@app.route('/api/cluster_products_by_price_rating', methods=['POST'])
def cluster_by_price_rating():
    """
    Cluster products based on discounted_price and rating.
    """
    data = request.get_json()
    features = ["discounted_price", "rating"]

    try:
        clustered_data = preprocess_and_cluster(data, features)

        for item in clustered_data:
            dp = item["features"]["discounted_price"]
            rt = item["features"]["rating"]
            if dp > 500 and rt < 3:
                item["description"] = "High Price, Low Rating"
            elif dp < 500 and rt > 4:
                item["description"] = "Low Price, High Rating"
            else:
                item["description"] = "Mid Price, Mid Rating"

        return jsonify(clustered_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/cluster_products_by_rating_count', methods=['POST'])
def cluster_by_rating_count():
    """
    Cluster products based on rating and rating_count.
    """
    data = request.get_json()
    features = ["rating", "rating_count"]

    try:
        clustered_data = preprocess_and_cluster(data, features)

        for item in clustered_data:
            rt = item["features"]["rating"]
            rc = item["features"]["rating_count"]
            if rt > 4 and rc > 10000:
                item["description"] = "High Rating, High Reviews"
            elif rt < 3 and rc < 1000:
                item["description"] = "Low Rating, Low Reviews"
            else:
                item["description"] = "Mid Rating, Mid Reviews"

        return jsonify(clustered_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/cluster_products_by_discount', methods=['POST'])
def cluster_by_discount():
    """
    Cluster products based on discount percentage.
    """
    data = request.get_json()
    features = ["discount_percentage"]

    try:
        clustered_data = preprocess_and_cluster(data, features)

        for item in clustered_data:
            dp = item["features"]["discount_percentage"]
            if dp > 50:
                item["description"] = "High Discount"
            elif 30 <= dp <= 50:
                item["description"] = "Medium Discount"
            else:
                item["description"] = "Low Discount"

        return jsonify(clustered_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    for path in DATASET_PATHS.values():
        if not os.path.exists(path):
            raise FileNotFoundError(f"Dataset not found: {path}")

    app.run(debug=True)
