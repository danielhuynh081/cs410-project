import os
import json
import datetime
from google.cloud import pubsub_v1
from validation import run_all_validations
from dotenv import load_dotenv

# GCP project and subscription
PROJECT_ID = "dataeng-project-assignment-1"
SUBSCRIPTION_ID = "breadcrumbs-sub"

# Set your service account credentials
os.environ["GOOGLE_APPLICATOIN CREDENTIALS"] = os.getenv("GOOGLE_CREDS_PATH")

# Create subscriber client and path
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def load_valid_vehicle_ids(filepath):
    with open(filepath, 'r') as file:
        lines = file.readlines()[1:]
        return set(int(line.strip()) for line in lines if line.strip().isdigit())

# tracking for inter-record checks
last_act_time_per_trip = {}
VALID_VEHICLE_IDS = load_valid_vehicle_ids("VehicleGroupsID.csv")

# Message handler
def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))

        if not run_all_validations(data):
            print("Message failed validation.")
            message.nack()
            return

        today = datetime.date.today().isoformat()
        filename = f"received_data_{today}.json"

        with open(filename, "a") as f:
            f.write(json.dumps(data) + "\n")

        print("Saved message to", filename)
        message.ack()
    except Exception as e:
        print("Error:", e)
        message.nack()

# Start receiving
print(f"📥 Listening on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()