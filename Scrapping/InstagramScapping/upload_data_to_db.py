from azure.cosmos import CosmosClient
import time
import uuid

ENDPOINT = "https://proiectnlp.documents.azure.com/"
KEY = "" # hidden for upload

client = CosmosClient(ENDPOINT, credential=KEY)

DATABASE_NAME = "base_nlp"
COMMENT_CONTAINER_NAME = "C3_INSTA"
VIDEO_CONTAINER_NAME = "Video"

db = client.get_database_client(DATABASE_NAME)


def extract_videos(filename):
    videos = []
    current_category = "unknown"

    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                clean_line = line.strip()
                if not clean_line:
                    continue

                if clean_line.startswith('http'):
                    videos.append({
                        "id": str(uuid.uuid4()),
                        "video_url": clean_line,
                        "category": current_category
                    })
                else:
                    # If it's not empty and not a link, it's a category header
                    current_category = clean_line

        return videos
    except FileNotFoundError:
        print(f"File '{filename}' not found.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def extract_raw_comments(filename):
    extracted_comments = []

    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                clean_line = line.strip()
                if not clean_line:
                    continue

                parts = clean_line.rsplit(maxsplit=2)
                if len(parts) >= 3:
                    extracted_comments.append(parts[0])
                else:
                    extracted_comments.append(clean_line)

        return extracted_comments
    except FileNotFoundError:
        print(f"File '{filename}' not found.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def dump_data_to_cosmos(videos, comments):
    video_container = db.get_container_client(VIDEO_CONTAINER_NAME)
    comment_container = db.get_container_client(COMMENT_CONTAINER_NAME)

    # Dump Videos
    print(f"Uploading {len(videos)} videos to {VIDEO_CONTAINER_NAME}...")
    for i, video_doc in enumerate(videos, 1):
        video_container.upsert_item(video_doc)
        if i % 50 == 0:
            print(f" -> Inserted {i} / {len(videos)} videos...")

    print("Done dumping videos.\n")

    # Dump Comments
    total_comments = len(comments)
    print(f"Uploading {total_comments} comments to {COMMENT_CONTAINER_NAME}...")

    start_time = time.time()

    for i, comment_text in enumerate(comments):
        # Every 300 comments get mapped to the respective video based on index
        video_index = i // 300

        # Failsafe in case there are slightly more comments than 300 * videos
        if video_index >= len(videos):
            video_index = len(videos) - 1

        linked_video_id = videos[video_index]["id"]

        document = {
            "id": str(uuid.uuid4()),
            "text": comment_text,
            "label": 2,
            "video_id": linked_video_id
        }

        comment_container.upsert_item(document)

        current_count = i + 1

        if current_count % 50 == 0:
            print(f" -> Inserted {current_count} / {total_comments} comments...")

        if current_count % 500 == 0:
            elapsed_time = time.time() - start_time
            avg_time_per_item = elapsed_time / current_count
            items_left = total_comments - current_count
            eta_seconds = items_left * avg_time_per_item

            mins, secs = divmod(int(eta_seconds), 60)
            print(f"   [⏳] Estimated time remaining: {mins}m {secs}s")

    print("Done dumping data.")


videos_list = extract_videos("insta_video_urls.txt")
comments_list = extract_raw_comments("out.txt")

if videos_list and comments_list:
    dump_data_to_cosmos(videos_list, comments_list)
else:
    print("Execution aborted: Make sure both text files are in the directory and contain data.")