import pandas as pd
import torch
import torch_directml
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import gc
from torch.nn.functional import softmax
from tqdm import tqdm

# 1. Initialize the AMD GPU (!! script doesn't work on nvidia -> used torch_directml)
dml_device = torch_directml.device(1)
print(f"Currently using: {torch_directml.device_name(1)[:-1]}")

# Load data
df = pd.read_csv("dataset_cleaned.csv")

# Drop any rows that might have become NaN during the save/load process
df = df.dropna(subset=['clean_text'])

# Force everything to a string just in case
texts = df['clean_text'].astype(str).tolist()

texts = df['clean_text'].tolist()


# 1. Emotion Tracking (Manual Loop for DirectML)
print("[...] Loading Emotion Model.")
model_name = "SamLowe/roberta-base-go_emotions"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name).to(dml_device)

batch_size = 64
all_emotions = []
all_scores = []

print("[...] Extracting emotions.")
for i in tqdm(range(0, len(texts), batch_size)):
    batch_texts = texts[i:i + batch_size]

    # Tokenize and move to the GPU manually
    inputs = tokenizer(batch_texts, return_tensors="pt", padding=True, truncation=True, max_length=512).to(dml_device)

    with torch.no_grad():
        outputs = model(**inputs)
        # Get the highest probability
        probs = softmax(outputs.logits, dim=1)
        scores, indices = torch.max(probs, dim=1)

        # Move back to CPU for list storage
        all_emotions.extend([model.config.id2label[idx.item()] for idx in indices])
        all_scores.extend(scores.cpu().tolist())

df['primary_emotion'] = all_emotions
df['emotion_confidence'] = all_scores

# 2. Feature Extraction (Embeddings) - DirectML Safe Version
print("[...] Loading Embedding Model.")
from sentence_transformers import SentenceTransformer

# Load on CPU first, then move to GPU
embedder = SentenceTransformer('all-mpnet-base-v2', device='cpu')
embedder.to(dml_device)

print("[...] Generating embeddings.")
embeddings_list = []

for i in tqdm(range(0, len(texts), batch_size)):
    batch_texts = texts[i:i + batch_size]

    with torch.no_grad():
        # Use the updated preprocess method to silence the warning
        features = embedder.preprocess(batch_texts)

        # Only move actual tensors to the GPU, ignore string metadata
        features = {
            k: v.to(dml_device) if isinstance(v, torch.Tensor) else v
            for k, v in features.items()
        }

        out_features = embedder.forward(features)
        batch_embeddings = out_features['sentence_embedding']

        embeddings_list.extend(batch_embeddings.cpu().numpy().tolist())

    # Cleanup VRAM
    del features, out_features, batch_embeddings
    gc.collect()

df['embedding'] = embeddings_list

# Save
df.to_parquet("social_media_modeled.parquet", index=False)
print(f" [:D] Done! Final count: {len(df)} rows.")