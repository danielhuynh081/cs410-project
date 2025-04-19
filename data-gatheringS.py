import csv
import urllib.request
import os
from datetime import datetime

# CSV file name
csv_file = '/home/vincle/data-gather/VehicleGroupsIDs.csv'  # Full path for cron

# Base URL
base_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

# Output directory
output_dir = 'downloads'
os.makedirs(output_dir, exist_ok=True)

count = 0
failed = 0

# Read vehicle IDs and download each JSON
with open(csv_file, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header
    for row in reader:
        if row:
            vehicle_id = row[0].strip()
            url = f"{base_url}{vehicle_id}"
            try:
                with urllib.request.urlopen(url) as response:
                    data = response.read().decode('utf-8')
                    filename = os.path.join(output_dir, f"{vehicle_id}.json")
                    with open(filename, 'w') as f:
                        f.write(data)
                    # Uncomment if you want to log each success
                    # print(f"Downloaded: {filename}")
                    count += 1
            except Exception as e:
                # Uncomment if you want to log each error
                # print(f"Failed for vehicle {vehicle_id}: {e}")
                failed += 1

end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Script finished at: {end_time}")
print(f"{count} files downloaded")
print(f"{failed} files not found")

