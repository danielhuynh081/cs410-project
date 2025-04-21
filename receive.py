import os
import json
import datetime
from google.cloud import pubsub_v1

# GCP project and subscription
PROJECT_ID = "dataeng-project-assignment-1"
SUBSCRIPTION_ID = "breadcrumbs-sub"

# Set your service account credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "dataeng-project-assignment-1-e99e41137ae5.json"

# Create subscriber client and path
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# Message handler
def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))
        today = datetime.date.today().isoformat()
        filename = f"received_data_{today}.json"

        # Append message to file
        with open(filename, "a") as f:
            f.write(json.dumps(data) + "\n")

        print("✅ Saved message to", filename)
        message.ack()
    except Exception as e:
        print("❌ Error:", e)
        message.nack()

# Start receiving
print(f"📥 Listening on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()