import time
from concurrent.futures import TimeoutError
from google.oauth2 import service_account
from google.cloud import pubsub_v1

# Service account file
SERVICE_ACCOUNT_FILE = "dataeng-project-assignment-1-e99e41137ae5.json"

# GCP settings
project_id = "dataeng-project-assignment-1"
subscription_id = "breadcrumbs-sub"
timeout = 60.0

# Load credentials
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

# Create subscriber client with credentials
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_counter = 0
start_time = time.time()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global message_counter
    message.ack()
    message_counter += 1

    if message_counter % 10000 == 0:
        print(f"💡 Received {message_counter} messages so far...")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"👂 Listening for messages on {subscription_path}...\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        end_time = time.time()
        streaming_pull_future.cancel()
        streaming_pull_future.result()

        print(f"\n✅ Finished receiving messages.")
        print(f"📦 Total messages received: {message_counter}")
        print(f"⏱️ Time taken to receive: {end_time - start_time:.2f} seconds")

