import os
import json
from datetime import datetime, timedelta
from db_utils import db_connect, insert_trip_and_breadcrumb

DATA_DIR = "/home/dahuynh/dataeng-pipeline"


def process_file(filepath, conn):
    count_loaded = 0
    prev_rows ={}
    with open(filepath, 'r') as f:
        for line in f:
            try:
                if not line.strip():
                    continue

                data = json.loads(line)

                # Only skip rows missing critical fields
                required_fields = ['EVENT_NO_TRIP', 'VEHICLE_ID', 'ACT_TIME', 'OPD_DATE', 'METERS']
                missing = [field for field in required_fields if data.get(field) is None]
                if missing:
                    print(f"\nSkipping row in {filepath}: missing fields -> {missing}")
                    print("Raw data:", data)
                    continue

                meters = int(data['METERS'])
                trip_id = int(data['EVENT_NO_TRIP'])
                vehicle_id = int(data['VEHICLE_ID'])
                latitude = float(data['GPS_LATITUDE']) if data.get('GPS_LATITUDE') is not None else None
                longitude = float(data['GPS_LONGITUDE']) if data.get('GPS_LONGITUDE') is not None else None
                act_time = int(data['ACT_TIME'])
                opd_date = data['OPD_DATE']

                # Timestamp/speed
                date_obj = datetime.strptime(opd_date.split(":")[0], "%d%b%Y")
                timestamp = date_obj + timedelta(seconds=act_time)
                
                speed = 0.0
                if trip_id in prev_rows:
                    prev = prev_rows[trip_id]
                    delta_meters = meters - prev['meters']
                    delta_time = act_time - prev['act_time']
                    if delta_time > 0:
                        speed = delta_meters / delta_time


                prev_rows[trip_id] = {'meters': meters, 'act_time': act_time}

                insert_trip_and_breadcrumb(conn, trip_id, vehicle_id, latitude, longitude, timestamp, speed)
                count_loaded += 1

                if count_loaded % 5000 == 0:
                    print(f"{count_loaded} records loaded from {filepath}")

            except Exception as e:
                print(f"Skipping row in {filepath}: {e}")
                continue

    print(f"Finished {filepath}: {count_loaded} records loaded.\n")


def main():
    conn = db_connect()
    for filename in sorted(os.listdir(DATA_DIR)):
        if filename.endswith(".json") and "breadcrumbs" in filename:
            filepath = os.path.join(DATA_DIR, filename)
            print(f"\nProcessing {filepath}")
            process_file(filepath, conn)
    conn.close()


if __name__ == "__main__":
    main()

