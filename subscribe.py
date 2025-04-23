import time
import json
from datetime import datetime
from concurrent.futures import TimeoutError
from google.oauth2 import service_account
from google.cloud import pubsub_v1

# === CONFIG ===
# SERVICE_ACCOUNT_FILE = "dataeng-project-assignment-1-e99e41137ae5.json"
SERVICE_ACCOUNT_FILE = '/home/dahuynh/dataeng-pipeline/dataeng-project-assignment-1-e99e41137ae5.json'
project_id = "dataeng-project-assignment-1"
subscription_id = "breadcrumbs-sub"
timeout = 60.0

# === SETUP ===
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

message_counter = 0
start_time = time.time()

# === CALLBACK FUNCTION ===
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global message_counter
    try:
        # Decode message and parse as JSON
        message_data = message.data.decode("utf-8")
        record = json.loads(message_data)

        # Create filename based on current date
        today_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"breadcrumbs-{today_str}.json"

        # Append the record to the file
        with open(filename, "a") as f:
            json.dump(record, f)
            f.write("\n")

        # Acknowledge and count
        message.ack()
        message_counter += 1

        if message_counter <= 5 or message_counter % 10000 == 0:
            print(f"📩 Received #{message_counter} message(s).")

    except Exception as e:
        print(f"❌ Error processing message: {e}")
        message.nack()

# === LISTENING ===
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"👂 Listening for messages on {subscription_path}...\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        end_time = time.time()
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        print(f"\n Finished receiving messages.")
        print(f" Total messages received: {message_counter}")
        print(f" Time taken: {end_time - start_time:.2f} seconds")

