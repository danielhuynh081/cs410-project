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

def load_valid_vehicle_ids(filepath):
    with open(filepath, 'r') as file:
        lines = file.readlines()[1:]
        return set(int(line.strip()) for line in lines if line.strip().isdigit())

# tracking for inter-record checks
last_act_time_per_trip = {}
VALID_VEHICLE_IDS = load_valid_vehicle_ids("VehicleGroupsID.csv")

# validate data
def validate_breadcrumb(data):
    #1. existence: EVENT_NO_TRIP must exist
    if 'EVENT_NO_TRIP' not in data or data['EVENT_NO_TRIP'] is None:
        print("Invalid: Missing EVENT_NO_TRIP")
        return False
    
    #2. existence: GPS_LATITUDE must exist
    if 'GPS_LATITUDE' not in data or data['GPS_LATITUDE'] is None:
        print("Invalid: Missing GPS_LATITUDE")
        return False
    
    #3. limit: longitude must be within -180 and 180
    if not (-180 <= data.get('GPS_LONGITUDE', 0) <= 180):
        print("Invalid: GPS_LATITUDE out of bounds")
        return False
    
    #4. limit: latitude must be within -90 and 90
    if not (-90 <= data.get('GPS_LATITUDE', 0) <= 90):
        print("Invalid: GPS_LATITUDE out of bounds")
        return False
    
    #5. intra-record: EVENT_NO_TRIP should not equal EVENT_NO_STOP
    if data.get('EVENT_NO_TRIP') == data.get('EVENT_NO_STOP'):
        print("Invalid: EVENT_NO_TRIP and EVENT_NO_STOP are the same")
        return False
    
    #6. inter-record: ACT_TIME should increase for the same trip
    trip_id = data.get('EVENT_NO_TRIP')
    act_time = data.get('ACT_TIME')
    if trip_id in last_act_time_per_trip:
        if act_time <= last_act_time_per_trip[trip_id]:
            print("Invalid: ACT_TIME not increasing for trip", trip_id)
            return False
        
    #7. referential integrity: VEHICLE_ID must be in valid list
    if data.get('VEHICLE_ID') not in VALID_VEHICLE_IDS:
        print("Invalid: VEHICLE_ID not recognized")
        return False
    
    #8. statistical: GPS_SATELITES should be between 5 and 20
    satellites = data.get('GPS_SATELLITES', 0)
    if not (5 <= satellites <= 20):
        print("Invalid: GPS_SATELLITES count is unrealistic")
        return False
    
    #9. summary: print the summary every 100 events
    if len(last_act_time_per_trip) % 100 == 0:
        print("Processed 100 events so far")

    #10. HDOP range check: should be between 0.5 and 10.0
    hdop = data.get('GPS_HDOP', 0)
    if not (0.5 <= hdop <= 10.0):
        print("Invalid: GPS_HDOP out of realistic range")
        return False
    
    # if everything passes,
    return True
    
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