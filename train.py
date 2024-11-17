import os
import json
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

# Set JAVA_HOME
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Big Data Training Pipeline") \
    .getOrCreate()

# Dataset paths
DATASET_PATHS = {
    'small': 'data/model_1.csv',
    'medium': 'data/model_2.csv',
    'full': 'data/model_3.csv'
}

# Output directory for models and statistics
OUTPUT_DIR = "models"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_and_preprocess_data(data_path):
    """
    Load and preprocess data with validation for missing values and ranges.
    """
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    numeric_columns = ["discounted_price", "actual_price", "discount_percentage", "rating", "rating_count"]
    
    # Cast numeric columns to proper types
    for col_name in numeric_columns:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
    
    # Handle missing values
    for col_name in numeric_columns:
        missing_count = df.filter(col(col_name).isNull()).count()
        if missing_count > 0:
            print(f"Filling {missing_count} missing values in column {col_name}.")
            mean_value = df.select(col_name).na.drop().groupBy().mean().first()[0]
            df = df.na.fill({col_name: mean_value})

    # Filter discount_percentage to be in range 0-100
    df = df.filter((col("discount_percentage") >= 0) & (col("discount_percentage") <= 100))

    return df

def validate_columns(df, required_columns):
    """
    Validate if all required columns exist in the DataFrame.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

def save_model_and_stats(model, scaler, data_path, features, dataset_name):
    """
    Train and save the clustering model along with its statistics.
    """
    df = load_and_preprocess_data(data_path)

    # Validate required columns
    validate_columns(df, features)

    # Step 1: Vectorize features
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    df_vectorized = assembler.transform(df)

    # Step 2: Scale features
    scaler_model = scaler.fit(df_vectorized)
    df_scaled = scaler_model.transform(df_vectorized)

    # Step 3: Cluster products
    clustered_df = model.fit(df_scaled).transform(df_scaled)

    # Save the model and cluster statistics
    model_path = os.path.join(OUTPUT_DIR, f"{dataset_name}_kmeans_model")
    stats_path = os.path.join(OUTPUT_DIR, f"{dataset_name}_cluster_stats.json")

    # Save model as a directory
    model.save(model_path)
    print(f"Model for {dataset_name} saved at {model_path}.")

    # Analyze clusters
    cluster_stats = clustered_df.groupBy("prediction").agg(
        {
            "discounted_price": "avg",
            "actual_price": "avg",
            "rating": "avg",
            "rating_count": "avg",
            "discount_percentage": "avg"
        }
    ).collect()

    # Save cluster statistics
    stats_dict = {row['prediction']: row.asDict() for row in cluster_stats}
    with open(stats_path, 'w') as f:
        json.dump(stats_dict, f, indent=4)
    
    print(f"Cluster statistics for {dataset_name} saved at {stats_path}.")

if __name__ == "__main__":
    features = ["discounted_price", "actual_price", "discount_percentage", "rating", "rating_count"]
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    kmeans = KMeans(k=3, seed=42, featuresCol="scaled_features")

    for dataset_name, data_path in DATASET_PATHS.items():
        print(f"Processing dataset: {dataset_name}")
        try:
            save_model_and_stats(kmeans, scaler, data_path, features, dataset_name)
        except Exception as e:
            print(f"Error processing dataset {dataset_name}: {e}")
