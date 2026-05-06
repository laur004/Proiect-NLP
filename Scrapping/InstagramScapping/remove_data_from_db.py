from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from azure.cosmos.partition_key import NonePartitionKeyValue
import time

ENDPOINT = "https://proiectnlp.documents.azure.com/"
KEY = "" # hidden for output

client = CosmosClient(ENDPOINT, credential=KEY)

DATABASE_NAME = "base_nlp"
COMMENT_CONTAINER_NAME = "C3_INSTA"
VIDEO_CONTAINER_NAME = "Video"

db = client.get_database_client(DATABASE_NAME)


def get_partition_key_info(container):
    properties = container.read()
    paths = properties['partitionKey']['paths']
    return paths[0] if paths else None


def get_pk_value(item, pk_path):
    if not pk_path:
        return NonePartitionKeyValue

    parts = pk_path.strip('/').split('/')
    val = item

    for part in parts:
        if isinstance(val, dict) and part in val:
            val = val[part]
        else:
            # Explicitly return the Cosmos DB NonePartitionKeyValue object
            return NonePartitionKeyValue

    return val


def clear_comments():
    container = db.get_container_client(COMMENT_CONTAINER_NAME)
    pk_path = get_partition_key_info(container)
    print(f"[{COMMENT_CONTAINER_NAME}] Detected Partition Key Path: {pk_path}")

    print(f"Fetching all items from {COMMENT_CONTAINER_NAME}...")
    items = list(container.query_items(
        query="SELECT * FROM c",
        enable_cross_partition_query=True
    ))

    print(f"Found {len(items)} comments. Deleting...")
    deleted_count = 0

    for i, item in enumerate(items):
        pk_value = get_pk_value(item, pk_path)
        try:
            container.delete_item(item=item['id'], partition_key=pk_value)
            deleted_count += 1
        except CosmosResourceNotFoundError:
            pass
        except Exception as e:
            print(f" -> Failed to delete comment {item['id']}: {e}")

        if (i + 1) % 500 == 0:
            print(f" -> Processed {i + 1} / {len(items)} comments...")

    print(f"Finished emptying {COMMENT_CONTAINER_NAME}. Successfully deleted {deleted_count} items.\n")


def clear_videos():
    container = db.get_container_client(VIDEO_CONTAINER_NAME)
    pk_path = get_partition_key_info(container)
    print(f"[{VIDEO_CONTAINER_NAME}] Detected Partition Key Path: {pk_path}")

    target_ids = [str(i) for i in range(50)]
    id_list_str = ", ".join(f"'{uid}'" for uid in target_ids)

    query = f"SELECT * FROM c WHERE c.id IN ({id_list_str})"

    print(f"Fetching videos with IDs 0-49 from {VIDEO_CONTAINER_NAME}...")
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))

    print(f"Found {len(items)} matching videos. Deleting...")
    for item in items:
        pk_value = get_pk_value(item, pk_path)
        try:
            container.delete_item(item=item['id'], partition_key=pk_value)
            print(f" -> Deleted video ID: {item['id']}")
        except CosmosResourceNotFoundError:
            print(f" -> Video ID {item['id']} already gone or not found.")
        except Exception as e:
            print(f" -> Failed to delete video {item['id']}: {e}")

    print(f"Finished processing target videos from {VIDEO_CONTAINER_NAME}.\n")


if __name__ == "__main__":
    start_time = time.time()

    clear_comments()
    clear_videos()

    elapsed = time.time() - start_time
    mins, secs = divmod(int(elapsed), 60)
    print(f"Cleanup complete in {mins}m {secs}s.")