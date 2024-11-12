import pandas as pd
from sklearn.cluster import KMeans

def train_kmeans_model(file_path, n_clusters, output_csv):
    # Load data
    df = pd.read_csv(file_path)
    
    # Select features for clustering (adjust based on your dataset)
    features = df[['discounted_price', 'rating_count', 'rating']].fillna(0)
    
    # Train KMeans model
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['cluster'] = kmeans.fit_predict(features)
    
    # Save results with cluster assignments
    df.to_csv(output_csv, index=False)
    print(f"Saved clustering results to {output_csv}")

# Run training for each model segment
train_kmeans_model('data/model_1.csv', n_clusters=3, output_csv='data/clustered_model_1.csv')
train_kmeans_model('data/model_2.csv', n_clusters=3, output_csv='data/clustered_model_2.csv')
train_kmeans_model('data/model_3.csv', n_clusters=3, output_csv='data/clustered_model_3.csv')
