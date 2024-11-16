import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col, mean
from pyspark.sql.types import DoubleType
import logging
import glob
import shutil
import numpy as np
from pyspark.sql.functions import collect_list

# Set Java home
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home'

# Configure logging to be minimal and clean
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)

def calculate_inertia(model, predictions):
    # Convert to numpy for inertia calculation
    centers = model.clusterCenters()
    features = np.array(predictions.select('scaled_features').collect())
    predictions_array = np.array(predictions.select('prediction').collect())
    
    inertia = 0
    for i in range(len(features)):
        center_id = predictions_array[i][0]
        center = centers[center_id]
        point = features[i][0]
        inertia += np.sum((point - center) ** 2)
    
    return inertia

def train_kmeans_model(spark, file_path, n_clusters, output_path):
    try:
        print(f"Loading data from {file_path}")
        df = spark.read.csv(file_path, header=True)
        
        # Convert string columns to double
        feature_cols = ['discounted_price', 'rating_count', 'rating']
        for col_name in feature_cols:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        # Handle missing values
        for col_name in feature_cols:
            mean_value = df.agg(mean(col(col_name)).alias('mean')).collect()[0]['mean']
            df = df.na.fill({col_name: mean_value})
        
        # Assemble and scale features
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_assembled = assembler.transform(df)
        
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        # Train KMeans model
        kmeans = KMeans(k=n_clusters, seed=42, featuresCol="scaled_features")
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        
        # Calculate metrics
        evaluator = ClusteringEvaluator(
            predictionCol='prediction',
            featuresCol='scaled_features',
            metricName='silhouette'
        )
        silhouette = evaluator.evaluate(predictions)
        inertia = calculate_inertia(model, predictions)
        
        print(f"Silhouette Score: {silhouette:.4f}")
        print(f"Inertia Score: {inertia:.4f}")
        
        # Prepare and save results
        final_df = predictions.select(
            *[col for col in df.columns],
            col("prediction").alias("cluster")
        )
        
        output_csv = output_path + '.csv'
        temp_dir = output_path + '_temp'
        
        final_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(temp_dir)
        
        temp_file = glob.glob(f"{temp_dir}/*.csv")[0]
        shutil.move(temp_file, output_csv)
        shutil.rmtree(temp_dir)
        
    except Exception as e:
        print(f"Error: {str(e)}")

def main():
    # Initialize Spark session with minimal logging
    spark = SparkSession.builder \
        .appName("KMeans Clustering") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    # Set log level to ERROR to minimize Spark output
    spark.sparkContext.setLogLevel("ERROR")
    
    # Run training for each model segment
    train_kmeans_model(spark, 'data/model_1.csv', n_clusters=3, output_path='data/clustered_model_1')
    train_kmeans_model(spark, 'data/model_2.csv', n_clusters=3, output_path='data/clustered_model_2')
    train_kmeans_model(spark, 'data/model_3.csv', n_clusters=3, output_path='data/clustered_model_3')
    
    spark.stop()

if __name__ == "__main__":
    main()