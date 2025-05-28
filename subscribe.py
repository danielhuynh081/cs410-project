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

# === CONFIG ===
SERVICE_ACCOUNT_FILE = '/home/dahuynh/dataeng-pipeline/dataeng-project-assignment-1-e99e41137ae5.json'
project_id = "dataeng-project-assignment-1"
subscription_id = "breadcrumbs-sub"
timeout = 600.0

# === SETUP ===
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
subscription_path = pubsub_v1.SubscriberClient(credentials=credentials).subscription_path(project_id, subscription_id)
conn = db_connect()
message_counter = 0

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global message_counter
    try:
        message_data = message.data.decode("utf-8")
        record = json.loads(message_data)

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
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
                VALUES (%s, NULL, %s, NULL, NULL)
                ON CONFLICT (trip_id) DO NOTHING;
            """, (row['trip_id'], row['vehicle_id']))

            cur.execute("""
                INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (row['tstamp'], row['latitude'], row['longitude'], row['speed'], row['trip_id']))

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
    print(f" Listening for messages on {subscription_path}...")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            print(f" Timeout reached, restarting subscription...")
            streaming_pull_future.cancel()
            streaming_pull_future.result()
