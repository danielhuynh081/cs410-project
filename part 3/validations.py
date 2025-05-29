def check_event_no_trip_exists(data):
    try:
        assert 'EVENT_NO_TRIP' in data and data['EVENT_NO_TRIP'] is not None, "Missing EVENT_NO_TRIP"
        return True
    except AssertionError as e:
        return False

def check_gps_latitude_exists(data):
    try:
        assert 'GPS_LATITUDE' in data and data['GPS_LATITUDE'] is not None, "Missing GPS_LATITUDE"
        return True
    except AssertionError as e:
        return False
    
    
def check_event_no_trip_exists(data):
    try:
        assert 'EVENT_NO_TRIP' in data and data['EVENT_NO_TRIP'] is not None, "Missing EVENT_NO_TRIP"
        return True
    except AssertionError as e:
        return False

def check_gps_latitude_exists(data):
    try:
        assert 'GPS_LATITUDE' in data and data['GPS_LATITUDE'] is not None, "Missing GPS_LATITUDE"
        return True
    except AssertionError as e:
        return False

def check_longitude_bounds(data):
    try:
        lon = float(data['GPS_LONGITUDE'])
        assert -180 <= lon <= 180, f"Longitude {lon} out of bounds"
        return True
    except (AssertionError, ValueError, TypeError) as e:
        return False

def check_latitude_bounds(data):
    try:
        lat = float(data['GPS_LATITUDE'])
        assert -90 <= lat <= 90, f"Latitude {lat} out of bounds"
        return True
    except (AssertionError, ValueError, TypeError) as e:
        return False

def check_speed_range(data):
    try:
        speed = float(data.get('GPS_SPEED', 0))
        assert 0 <= speed <= 200, f"Unrealistic GPS_SPEED: {speed}"
        return True
    except (AssertionError, ValueError, TypeError) as e:
        return False

def check_satellite_range(data):
    try:
        sats = int(data.get('GPS_SATS', 0))
        assert 0 <= sats <= 50, f"Invalid number of satellites: {sats}"
        return True
    except (AssertionError, ValueError, TypeError) as e:
        return False

def check_hdop_range(data):
    try:
        hdop = float(data.get('GPS_HDOP', 0))
        assert 0 <= hdop <= 50, f"Invalid GPS_HDOP: {hdop}"
        return True
    except (AssertionError, ValueError, TypeError) as e:
        return False

# Stub functions for validations that depend on external data/context
def check_trip_vs_stop(data):
    print("check_trip_vs_stop requires context. Stub returning True.")
    return True

def check_act_time_progression(data):
    print("check_act_time_progression requires context. Stub returning True.")
    return True

def check_vehicle_id_valid(data):
    print("check_vehicle_id_valid requires context. Stub returning True.")
    return True

def run_all_validations(data):
    return all([
        check_event_no_trip_exists(data),
        check_gps_latitude_exists(data),
        check_longitude_bounds(data),
        check_latitude_bounds(data),
        check_speed_range(data),
        check_satellite_range(data),
        check_hdop_range(data),
        check_trip_vs_stop(data),
        check_act_time_progression(data),
        check_vehicle_id_valid(data),
    ])
