from azure.cosmos import CosmosClient
import pandas as pd

# Credentials
ENDPOINT = "https://proiectnlp.documents.azure.com/"
KEY = ""  # key hidden for upload
DATABASE_NAME = "base_nlp"

client = CosmosClient(ENDPOINT, credential=KEY)
db = client.get_database_client(DATABASE_NAME)


def extract_and_compile_data():
    # Fetch Video Metadata
    print("[...] Fetching video metadata.")
    video_container = db.get_container_client("Video")
    # Using enable_cross_partition_query=True since we are doing a full table scan
    videos = list(video_container.query_items(
        query="SELECT c.id, c.category, c.video_url FROM c",
        enable_cross_partition_query=True
    ))

    video_lookup = {
        v['id']: {'category': v.get('category'), 'url': v.get('video_url')}
        for v in videos
    }
    print(f" [:D] Loaded {len(video_lookup)} videos!\n\n")

    # Fetch Comments from all platforms
    comment_containers = ["C1_YT", "C2_FB", "C3_INSTA", "C3_TK"]
    all_comments = []

    for container_name in comment_containers:
        print(f"[...] Fetching comments from {container_name}.")
        container = db.get_container_client(container_name)

        comments = list(container.query_items(
            query="SELECT c.id, c.text, c.label, c.video_id FROM c",
            enable_cross_partition_query=True
        ))

        print(f"  [?] Found {len(comments)} comments in {container_name}.\n")

        # Attach video category and URL to each comment
        for comment in comments:
            vid_info = video_lookup.get(comment.get('video_id'), {})

            all_comments.append({
                "comment_id": comment.get("id"),
                "text": comment.get("text"),
                "label": comment.get("label"),  # 0=YT, 1=FB, 2=INSTA, 3=TK
                "platform": container_name.split("_")[1],  # clean platform name (YT, FB, etc.)
                "video_id": comment.get("video_id"),
                "category": vid_info.get("category", "unknown"),
                "video_url": vid_info.get("url", "unknown")
            })

    print(f" [:D] Total comments extracted: {len(all_comments)}!")
    return all_comments


raw_data = extract_and_compile_data()

# Convert the list of dictionaries directly into a Pandas DataFrame
df = pd.DataFrame(raw_data)

# Save to CSV for easy inspection
csv_filename = "dataset_raw.csv"
df.to_csv(csv_filename, index=False, encoding='utf-8')
print(f" [:D] Done! Data saved to \"{csv_filename}\".")