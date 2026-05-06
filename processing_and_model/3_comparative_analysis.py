import pandas as pd
import numpy as np
from scipy.spatial.distance import cosine

# Load the modeled data
df = pd.read_parquet("social_media_modeled.parquet")

# 1. Centroid Calculation
print("[...] Calculating platform centroids.")
centroids = {}

# Group by platform and calculate the mean embedding vector
for platform, group in df.groupby('platform'):
    # Stack the lists into a 2D numpy array, then take the mean across the columns
    platform_matrix = np.vstack(group['embedding'].values)
    centroids[platform] = platform_matrix.mean(axis=0)


# Cosine Similarity Matrix
print("[...] Generating Cosine Similarity Matrix.")
platforms = list(centroids.keys())

# Create an empty dataframe to hold the 4x4 matrix
sim_matrix = pd.DataFrame(index=platforms, columns=platforms, dtype=float)

for p1 in platforms:
    for p2 in platforms:
        # SciPy calculates distance (0 = identical, 1 = orthogonal).
        # We subtract from 1 to get Similarity (1 = identical, 0 = unrelated)
        sim = 1 - cosine(centroids[p1], centroids[p2])
        sim_matrix.loc[p1, p2] = round(sim, 4)

print("\n--- Platform Semantic Similarity ---")
print(sim_matrix)


# Emotion Distribution
print("\n[...] Calculating Emotion Distributions.")

# Count occurrences of each emotion per platform
emotion_counts = df.groupby(['platform', 'primary_emotion']).size().unstack(fill_value=0)

# Divide each row by its total sum to get percentages, then multiply by 100
emotion_dist = emotion_counts.div(emotion_counts.sum(axis=1), axis=0) * 100
emotion_dist = emotion_dist.round(2)

print("\n--- Emotion Breakdown (%) ---")
print(emotion_dist)

# Optional: Save these aggregations if you want to plot them elsewhere
sim_matrix.to_csv("platform_similarity_matrix.csv")
emotion_dist.to_csv("platform_emotion_distribution.csv")