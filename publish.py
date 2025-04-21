import os, json
from concurrent import futures
from google.oauth2 import service_account
from google.cloud import pubsub_v1

# ✅ Replace with your real service account key file
SERVICE_ACCOUNT_FILE = "dataeng-project-assignment-1-e99e41137ae5.json"

# ✅ Your actual GCP project ID
PROJECT_ID = "dataeng-project-assignment-1"

# ✅ Your Pub/Sub topic name
TOPIC_ID = "breadcrumbs"

def future_callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"An error occurred: {e}")

# Load service account credentials
pubsub_creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

# Create the Pub/Sub publisher
publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# ✅ Load your data file
with open("my_data.json", "r") as f:
    records = json.load(f)

future_list = []
count = 0

# Publish each breadcrumb reading
for record in records:
    data_str = json.dumps(record)
    data = data_str.encode()
    future = publisher.publish(topic_path, data)
    future.add_done_callback(future_callback)
    future_list.append(future)
    count += 1
    if count % 50000 == 0:
        print(f"{count} records queued...")

# Wait for all publishing futures to complete
for future in futures.as_completed(future_list):
    continue

print("✅ All records published.")

