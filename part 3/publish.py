import urllib.request
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import json
from datetime import datetime
from google.cloud import pubsub_v1
from google.oauth2 import service_account

class StopEventPipeline:
    def __init__(self, service_account_file, project_id, topic_id, vehicle_ids):
        self.project_id = project_id
        self.topic_id = topic_id
        self.vehicle_ids = vehicle_ids
        self.base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="

        creds = service_account.Credentials.from_service_account_file(service_account_file)
        self.publisher = pubsub_v1.PublisherClient(credentials=creds)
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def fetch_and_parse(self, vehicle_id):
        url = f"{self.base_url}{vehicle_id}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urllib.request.urlopen(req) as response:
            html = response.read().decode("utf-8")
            soup = BeautifulSoup(html, "html.parser")
            tables = soup.find_all("table")  # ← you need this line

            if not tables:
                raise ValueError("No <table> found in response")

            # Read the first HTML table into a DataFrame
            df = pd.read_html(StringIO(str(tables[0])))[0]
            df.columns = [col.lower().strip() for col in df.columns]
            df["vehicle_number"] = vehicle_id
            return df.to_dict(orient="records")

    def publish_data(self, records):
        futures = []
        for record in records:
            encoded = json.dumps(record).encode("utf-8")
            future = self.publisher.publish(self.topic_path, encoded)
            futures.append(future)
        return futures

    def run(self):
        total_records = 0
        failed = 0
        all_futures = []
        start_time = datetime.now()

        print(f"[{start_time}] Starting Stop Event Pipeline...")

        for vid in self.vehicle_ids:
            try:
                records = self.fetch_and_parse(vid)
                if records:
                    all_futures.extend(self.publish_data(records))
                    total_records += len(records)
                    print(f"✔ Vehicle {vid}: {len(records)} records published")
                else:
                    print(f" Vehicle {vid}: No data returned")
            except Exception as e:
                print(f" Vehicle {vid} failed: {e}")
                failed += 1

        print(" Waiting for all publish futures to complete...")
        for future in all_futures:
            future.result()

        end_time = datetime.now()
        print(f"\n Finished publishing.\nTotal records: {total_records}\nFailures: {failed}\nDuration: {(end_time - start_time).total_seconds():.2f} seconds")

if __name__ == "__main__":
    vehicle_ids = [
        2905, 2907, 2908, 2916, 2918, 2923, 2926, 2927, 2928, 2934,
        2935, 2939, 3008, 3010, 3013, 3017, 3018, 3019, 3023, 3033,
        3037, 3044, 3045, 3051, 3053, 3054, 3059, 3105, 3108, 3110,
        3113, 3114, 3118, 3126, 3127, 3128, 3130, 3143, 3145, 3148,
        3154, 3157, 3163, 3201, 3206, 3209, 3213, 3217, 3219, 3222,
        3224, 3225, 3229, 3231, 3233, 3239, 3243, 3245, 3246, 3248,
        3252, 3256, 3267, 3301, 3303, 3304, 3308, 3310, 3313, 3314,
        3320, 3326, 3327, 3401, 3404, 3405, 3407, 3411, 3419, 3420,
        3422, 3511, 3515, 3516, 3517, 3518, 3520, 3524, 3525, 3531,
        3533, 3534, 3538, 3542, 3544, 3545, 3547, 3554, 3559, 3560,
        3564, 3566, 3568, 3570, 3571, 3573, 3574, 3601, 3606, 3608,
        3615, 3623, 3627, 3629, 3631, 3632, 3646, 3650, 3704, 3710,
        3714, 3718, 3727, 3730, 3731, 3733, 3736, 3741, 3744, 3757,
        3902, 3903, 3909, 3911, 3912, 3915, 3916, 3921, 3927, 3929,
        3930, 3931, 3933, 3938, 3940, 3944, 3953, 3954, 3955, 3961,
        3963, 3964, 4002, 4003, 4006, 4014, 4015, 4016, 4018, 4020,
        4022, 4023, 4026, 4033, 4036, 4038, 4040, 4043, 4045, 4047,
        4051, 4058, 4059, 4066, 4067, 4069, 4202, 4203, 4206, 4207,
        4208, 4212, 4215, 4217, 4218, 4219, 4222, 4224, 4227, 4228,
        4230, 4231, 4234, 4305, 4510, 4515, 4516, 4522, 4526, 99222
    ]

    pipeline = StopEventPipeline(
        service_account_file="dataeng-project-assignment-1-e99e41137ae5.json",
        project_id="dataeng-project-assignment-1",
        topic_id="stopdatatopic",
        vehicle_ids=vehicle_ids
    )

    pipeline.run()

