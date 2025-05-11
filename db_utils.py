import psycopg2

def db_connect():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="12345",
        host="localhost"
    )
def insert_trip_and_breadcrumb(conn, trip_id, vehicle_id, latitude, longitude, tstamp, speed):
    try:
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
    except Exception as e:
        conn.rollback()  # Recover connection state
        print(f"DB insert error (trip_id={trip_id}): {e}")

