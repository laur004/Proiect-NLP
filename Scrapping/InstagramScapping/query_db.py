from azure.cosmos import CosmosClient

ENDPOINT = "https://proiectnlp.documents.azure.com/"
KEY = "" # key hidden for upload

client = CosmosClient(ENDPOINT, credential=KEY)
db = client.get_database_client("base_nlp")
video_container = db.get_container_client("Video")
comment_container = db.get_container_client("C3_INSTA")

TARGET_CATEGORY = 'comedy'

# Find all video IDs that belong to the target category
print(f"Finding videos in category: '{TARGET_CATEGORY}'...")
video_query = f"SELECT c.id FROM c WHERE c.category = '{TARGET_CATEGORY}'"

videos = list(video_container.query_items(
    query=video_query,
    enable_cross_partition_query=True
))

video_ids = [v['id'] for v in videos]

if not video_ids:
    print(f"No videos found for category '{TARGET_CATEGORY}'.")
else:
    print(f"Found {len(video_ids)} videos. Fetching their comments...")

    # Query comments using the IN clause with the extracted video IDs
    # Format the IDs into a SQL-friendly string like: 'id1', 'id2', 'id3'
    id_list_str = ", ".join(f"'{vid}'" for vid in video_ids)

    comment_query = f"SELECT c.id, c.text, c.video_id FROM c WHERE c.video_id IN ({id_list_str})"

    comments = list(comment_container.query_items(
        query=comment_query,
        enable_cross_partition_query=True
    ))

    print(f"Found {len(comments)} comments across these videos.\n")

    # Limiting the print output to the first 20 so it doesn't flood your console if there are thousands
    print("--- Sample of Comments ---")
    for comment in comments[:20]:
        print(f"Video ID: {comment.get('video_id')} | Comment: {comment.get('text')}")

    if len(comments) > 20:
        print(f"... and {len(comments) - 20} more.")