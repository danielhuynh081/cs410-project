import os
import pandas as pd
import io
from db_utils import db_connect

DATA_DIR = "/home/dahuynh/dataeng-pipeline"

def process_file(filepath, conn):
    print(f"\n Processing: {filepath}")
    try:
        df = pd.read_json(filepath)

        # Convert timestamp
        df['TIMESTAMP'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce') + pd.to_timedelta(df['ACT_TIME'], unit='s')
        df = df.drop(columns=['OPD_DATE', 'ACT_TIME'])

        # Calculate speed
        df['dMETERS'] = df['METERS'].diff()
        df['dTIMESTAMP'] = df['TIMESTAMP'].diff().dt.total_seconds()
        df['SPEED'] = df['dMETERS'] / df['dTIMESTAMP']
        df.at[0, 'SPEED'] = df.at[1, 'SPEED']
        df = df.drop(columns=['dMETERS', 'dTIMESTAMP'])

        # Select + rename columns for DB
        df = df[['EVENT_NO_TRIP', 'VEHICLE_ID', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'TIMESTAMP', 'SPEED']]
        df.columns = ['trip_id', 'vehicle_id', 'latitude', 'longitude', 'tstamp', 'speed']

        # Insert unique trip_ids into trip table
        trip_ids = df[['trip_id', 'vehicle_id']].drop_duplicates()
        with conn.cursor() as cur:
            for _, row in trip_ids.iterrows():
                cur.execute("""
                    INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
                    VALUES (%s, NULL, %s, NULL, NULL)
                    ON CONFLICT (trip_id) DO NOTHING;
                """, (row['trip_id'], row['vehicle_id']))
        conn.commit()

        # Convert breadcrumb rows to CSV (in memory)
        csv_buf = io.StringIO()
        df[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']].to_csv(csv_buf, index=False, header=False)
        csv_buf.seek(0)

        # Bulk insert to breadcrumb
        with conn.cursor() as cur:
            cur.copy_from(csv_buf, 'breadcrumb', sep=',')
        conn.commit()

        print(f" {len(df)} breadcrumbs loaded for file: {os.path.basename(filepath)}")

    except Exception as e:
        conn.rollback()
        print(f" Failed to process {filepath}: {e}")

def main():
    conn = db_connect()
    for filename in sorted(os.listdir(DATA_DIR)):
        if filename.endswith(".json") and "breadcrumbs" in filename:
            filepath = os.path.join(DATA_DIR, filename)
            process_file(filepath, conn)
    conn.close()

if __name__ == "__main__":
    main()
                                                                                                                                                                                                                                                                                                                                   65,0-1        Bot
                                                                                                                                                                       11,21         Top
