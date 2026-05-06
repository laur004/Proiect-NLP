import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from math import pi



def plot_similarity_heatmap(sim_matrix):
    plt.figure(figsize=(8, 6))

    # Dynamically find the lowest value that isn't 1.0 (the diagonal)
    min_val = sim_matrix[sim_matrix < 1.0].min().min()

    sns.heatmap(sim_matrix, annot=True, fmt=".2%", cmap="YlGnBu",
                vmin=min_val, vmax=0.97,
                cbar_kws={'label': 'Cosine Similarity'})

    plt.title('Platform Semantic Similarity', pad=20, fontsize=14)
    plt.tight_layout()
    plt.savefig('graphs/heatmap_similarity_zoomed.png', dpi=300)
    plt.show()


def plot_emotion_radar(emotion_dist):
    target_emotions = ['admiration', 'amusement', 'anger', 'sadness', 'surprise']
    df_radar = emotion_dist[target_emotions]

    # Calculate the mean of each emotion across all platforms
    average_emotions = df_radar.mean()

    # Divide each platform's score by the average to get an Index
    # (1.0 = exactly average, 1.5 = 50% higher than average)
    df_indexed = df_radar / average_emotions

    categories = list(df_indexed.columns)
    N = len(categories)
    angles = [n / float(N) * 2 * pi for n in range(N)]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
    ax.set_theta_offset(pi / 2)
    ax.set_theta_direction(-1)
    plt.xticks(angles[:-1], [cat.capitalize() for cat in categories], size=12)

    colors = ['#ff00ff', '#00ff00', '#ff0000', '#0000ff']
    platforms = df_indexed.index.tolist()

    for i, platform in enumerate(platforms):
        values = df_indexed.loc[platform].values.flatten().tolist()
        values += values[:1]

        ax.plot(angles, values, linewidth=2, linestyle='solid', label=platform, color=colors[i % len(colors)])
        ax.fill(angles, values, color=colors[i % len(colors)], alpha=0.1)

    # Draw a reference line for "Average" (1.0)
    ax.plot(angles, [1.0] * len(angles), color='gray', linestyle='dashed', linewidth=1, label="Average")

    plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))
    plt.title('Relative Emotional Over-Indexing by Platform', pad=30, fontsize=14)
    plt.tight_layout()
    plt.savefig('graphs/radar_emotions_indexed.png', dpi=300)
    plt.show()


def plot_sentiment_bars(emotion_dist):
    positive_labels = ['admiration', 'amusement', 'approval', 'caring', 'desire',
                       'excitement', 'gratitude', 'joy', 'love', 'optimism', 'pride', 'relief']
    negative_labels = ['anger', 'annoyance', 'disappointment', 'disapproval', 'disgust',
                       'embarrassment', 'fear', 'grief', 'nervousness', 'remorse', 'sadness']
    neutral_labels = ['confusion', 'curiosity', 'realization', 'surprise', 'neutral']

    sentiment_df = pd.DataFrame(index=emotion_dist.index)

    pos_cols = [col for col in positive_labels if col in emotion_dist.columns]
    neg_cols = [col for col in negative_labels if col in emotion_dist.columns]
    neu_cols = [col for col in neutral_labels if col in emotion_dist.columns]

    sentiment_df['Positive'] = emotion_dist[pos_cols].sum(axis=1)
    sentiment_df['Negative'] = emotion_dist[neg_cols].sum(axis=1)
    sentiment_df['Neutral'] = emotion_dist[neu_cols].sum(axis=1)

    # Normalize to 100%
    sentiment_df = sentiment_df.div(sentiment_df.sum(axis=1), axis=0) * 100

    # Sort the dataframe from highest Positive to lowest
    sentiment_df = sentiment_df.sort_values(by='Positive', ascending=False)

    colors = ['#2ca02c', '#d62728']
    ax = sentiment_df[['Positive', 'Negative']].plot(
        kind='bar', stacked=True, figsize=(10, 6), color=colors, edgecolor='white'
    )

    plt.title('Polarized Sentiment', fontsize=14, pad=15)
    plt.ylabel('Percentage (%)')
    plt.xlabel('Platform')
    plt.xticks(rotation=0)

    # Calculate the tallest bar (Positive + Negative) and set the limit with a 5% buffer
    max_height = (sentiment_df['Positive'] + sentiment_df['Negative']).max()
    plt.ylim(0, max_height * 1.05)

    plt.legend(title='Sentiment', bbox_to_anchor=(1.05, 1), loc='upper left')

    for c in ax.containers:
        ax.bar_label(c, fmt='%.1f%%', label_type='center', color='white', fontsize=10, weight='bold')

    plt.tight_layout()
    plt.savefig('graphs/stacked_sentiment.png', dpi=300)
    plt.show()


sim_matrix = pd.read_csv("platform_similarity_matrix.csv", index_col=0)
emotion_dist = pd.read_csv("platform_emotion_distribution.csv", index_col=0)

print("[...] Generating Heatmap.")
plot_similarity_heatmap(sim_matrix)

print("[...] Generating Radar Chart.")
plot_emotion_radar(emotion_dist)

print("[...] Generating Stacked Bar Chart.")
plot_sentiment_bars(emotion_dist)