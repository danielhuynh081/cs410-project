import time
import json
import io
import os
import pandas as pd
from datetime import datetime
from concurrent.futures import TimeoutError
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from db_utils import db_connect
from validations import run_all_validations

# === CONFIG ===
SERVICE_ACCOUNT_FILE = '/home/dahuynh/stop-data-pipeline/dataeng-project-assignment-1-e99e41137ae5.json'
PROJECT_ID = "dataeng-project-assignment-1"
SUBSCRIPTION_ID = "stopdatatopic-sub"
TIMEOUT = 600.0

# === SETUP ===
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
subscription_path = pubsub_v1.SubscriberClient(credentials=credentials).subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
conn = db_connect()
message_counter = 0

# === CALLBACK ===
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global message_counter
    try:
        record = json.loads(message.data.decode("utf-8"))

        #  Step 1: Validation
        if not run_all_validations(record):
            print(" Record failed validation. Skipping...")
            message.ack()
            return

        #  Step 2: Transformation
        df = pd.DataFrame([record])
        df['TIMESTAMP'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce') + pd.to_timedelta(df['ACT_TIME'], unit='s')
        df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])

        df['dMETERS'] = df['METERS'].diff()
        df['dTIMESTAMP'] = df['TIMESTAMP'].diff().dt.total_seconds()
        df['SPEED'] = df['dMETERS'] / df['dTIMESTAMP']
        df.at[0, 'SPEED'] = 0.0
        df = df.drop(columns=['dMETERS', 'dTIMESTAMP'])

        df = df[['EVENT_NO_TRIP', 'VEHICLE_ID', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'TIMESTAMP', 'SPEED']]
        df.columns = ['trip_id', 'vehicle_id', 'latitude', 'longitude', 'tstamp', 'speed']
        row = df.iloc[0]

        #  Step 3: Convert to native types
        trip_id = int(row['trip_id'])
        vehicle_id = int(row['vehicle_id'])
        latitude = float(row['latitude'])
        longitude = float(row['longitude'])
        tstamp = row['tstamp'].to_pydatetime()
        speed = float(row['speed'])

        #  Step 4: Insert into DB
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
                VALUES (%s, NULL, %s, NULL, NULL)
                ON CONFLICT (trip_id) DO NOTHING;
            """, (trip_id, vehicle_id))

            cur.execute("""
                INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (tstamp, latitude, longitude, speed, trip_id))

        conn.commit()
        message.ack()
        message_counter += 1

        if message_counter <= 5 or message_counter % 1000 == 0:
            print(f" Stored message #{message_counter}")

    except Exception as e:
        conn.rollback()
        print(f" Error processing message: {e}")
        message.nack()

# === LISTENING LOOP ===
while True:
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f" Listening for messages on {subscription_path}...\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=TIMEOUT)
        except TimeoutError:
            print(" Timeout reached. Restarting subscriber...")
            streaming_pull_future.cancel()
            streaming_pull_future.result()                                                                                                                                                
                                                                                                                                                              
