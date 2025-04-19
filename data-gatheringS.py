import csv
import urllib.request
import os

# CSV file name
csv_file = 'VehicleGroupsIDs.csv'  # Make sure this is in the same directory

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
                    print(f"Downloaded: {filename}")
                    count += 1
            except Exception as e:
                print(f"Failed for vehicle {vehicle_id}: {e}")
                failed += 1

print(f"{count} files downloaded")
print(f"{failed} files not found")
