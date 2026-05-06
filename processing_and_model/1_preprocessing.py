import pandas as pd
import html
import re
import emoji
from langdetect import detect, LangDetectException

df = pd.read_csv("dataset_raw.csv")


def clean_comment(text):
    if not isinstance(text, str):
        return ""

    # Strip out HTML tags, replacing them with a space (e.g., <br> becomes a space)
    text = re.sub(r'<[^>]+>', ' ', text)

    # Decode HTML entities (e.g., &#39; becomes ')
    text = html.unescape(text)

    # Remove the TikTok [Sticker] placeholder (case-insensitive)
    text = re.sub(r'\[sticker\]', '', text, flags=re.IGNORECASE)

    # Remove URLs and @mentions
    text = re.sub(r'http\S+|www\.\S+', '', text)
    text = re.sub(r'@\w+', '', text)

    # Demojize
    text = emoji.demojize(text, delimiters=(" :", ": "))

    # turning fancy apostrophes to standard straight ones
    text = text.replace('’', "'").replace('‘', "'")

    # Strip standard punctuation, BUT KEEP !, ?, ., ' and ,
    text = re.sub(r"[^a-zA-Z0-9\s_:\!\?\.,'’]", '', text)

    # Collapse multiple spaces into a single space (cleanup from removing tags/stickers/emohis)
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def is_valid_english_or_emoji(text):
    # Temporarily remove the demojized emoji tags (added 0-9 for emojis like :100:)
    text_without_emojis = re.sub(r':[a-z0-9_]+:', '', text)

    # Extract just the alphabetical words left behind
    words_only = re.sub(r'[^a-z\s]', '', text_without_emojis).strip()

    # Condition 1: Pure emojis, numbers, or punctuation -> Keep it
    if len(words_only) == 0:
        return True

    # Condition 2: Very short internet slang (1-2 words) -> Keep it
    # langdetect fails on things like "lol" or "wow", so we bypass it.
    if len(words_only.split()) <= 2:
        return True

    # Condition 3: Actual sentences -> Run language detection
    try:
        return detect(text_without_emojis) == 'en'
    except LangDetectException:
        return False


print(f"  [?] Original row count: {len(df)}")

print("[...] Cleaning text and handling emojis")
df['clean_text'] = df['text'].apply(clean_comment)

# Drop rows that ended up completely empty
df = df[df['clean_text'].str.len() > 1]

print("[...] Filtering out non-English comments (whilst also preserving emoji-only comments).")
df['is_valid'] = df['clean_text'].apply(is_valid_english_or_emoji)
df = df[df['is_valid'] == True]

df = df.drop(columns=['is_valid'])

print(f" [:D] Done! Cleaned valid row count: {len(df)}.")

df.to_csv("dataset_cleaned.csv", index=False)