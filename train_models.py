import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def train_kmeans_model(file_path, n_clusters, output_csv):
    try:
        # Load data
        logging.info(f"Loading data from {file_path}")
        df = pd.read_csv(file_path)
        
        # Select and preprocess features for clustering
        features = df[['discounted_price', 'rating_count', 'rating']].copy()
        features = features.astype(np.float64)
        
        # Handle missing values
        missing_values = features.isnull().sum().sum()
        if missing_values > 0:
            logging.info(f"Handling {missing_values} missing values by filling with column mean.")
            features.fillna(features.mean(), inplace=True)
        
        # Scale features (optional: depending on data distribution)
        logging.info("Preprocessing data (scaling features).")
        features = (features - features.mean()) / features.std()
        
        # Initialize and train KMeans model
        logging.info(f"Training KMeans model with n_clusters={n_clusters}")
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        clusters = kmeans.fit_predict(features)
        
        # Calculate clustering metrics
        inertia = kmeans.inertia_
        silhouette = silhouette_score(features, clusters)
        logging.info(f"KMeans model trained with inertia: {inertia:.4f} and silhouette score: {silhouette:.4f}")
        
        # Append cluster labels to the original dataframe
        df['cluster'] = clusters
        
        # Save results with cluster assignments
        df.to_csv(output_csv, index=False)
        logging.info(f"Clustering results saved to {output_csv}")
        
    except Exception as e:
        logging.error(f"An error occurred while training the model: {e}")

# Run training for each model segment
train_kmeans_model('data/model_1.csv', n_clusters=3, output_csv='data/clustered_model_1.csv')
train_kmeans_model('data/model_2.csv', n_clusters=3, output_csv='data/clustered_model_2.csv')
train_kmeans_model('data/model_3.csv', n_clusters=3, output_csv='data/clustered_model_3.csv')
