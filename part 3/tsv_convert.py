#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point

features = []

with open('trip_data.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    next(reader)  # skip header row

    for row in reader:
        if len(row) < 3:
            continue

        x = row[0]
        y = row[1]
        speed = row[2]

        if speed is None or speed.strip() == "":
            continue

        try:
            latitude, longitude = map(float, (y, x))
            features.append(
                Feature(
                    geometry=Point((longitude, latitude)),
                    properties={'speed': int(speed)}
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data.geojson", "w") as f:
    f.write('%s' % collection)

