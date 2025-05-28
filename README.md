# cs410-project

### A.Create, configure and use a Google Cloud Platform (GCP) linux virtual machine (VM). 
### B.Develop a simple python program to gather the data programmatically. 
### C.Configure your VM to have the correct timezone set (PST/PDT - time of year dependent). 
### D.Configure your VM running your gathering client to run daily. daniel
### E.Allocate and configure a message passing “topic” and “subscription” at Google Cloud Pub/Sub.
### F.Enhance your data gathering client to parse the breadcrumb data and publish individual JSON records.
### G.Enhance your data gathering client to send the individual breadcrumb records to your Cloud Pub/Sub topic. 
### H.Develop a python program to receive the breadcrumb readings from the Pub/Sub topic and save them to file, one file per day.
### I.Configure your VM to run your Pub/Sub receiver constantly so that it always receives all new data. 
### J.Schedule your VM to start and stop automatically. 

# Vincent

### A. Create, configure and use a Google Cloud Platform (GCP) linux virtual machine (VM).
### B. Develop a simple python program to gather the data programmatically.
### C. Configure your VM to have the correct timezone set (PST/PDT - time of year dependent).
### D. Configure your VM running your gathering client to run daily.


# Daniel

### E. Allocate and configure a message passing “topic” and “subscription” at Google Cloud Pub/Sub.
### F. Enhance your data gathering client to parse the breadcrumb data and publish individual JSON records. 
### G. Enhance your data gathering client to send the individual breadcrumb records to your Cloud Pub/Sub topic.


# Sal

### H. Develop a python program to receive the breadcrumb readings from the Pub/Sub topic and save them to file, one file per day.
### I. Configure your VM to run your Pub/Sub receiver constantly so that it always receives all new data.
### J. Schedule your VM to start and stop automatically.


# Due Sunday 4/20/25

## Environment Setup

To run this project locally, each developer must provide their own Google Cloud service account credentials.

1. Place your credentials JSON file inside a `creds/` folder in the project root.

2. Create a `.env` file (not tracked by Git) in the project root with the following content:

```
GOOGLE_CREDS_PATH=creds/dataeng-project-assignment-1-e99e41137ae5.json
```

3. The project will automatically load this path using `python-dotenv`.

**Note:** Do not commit your `.env` file or credentials JSON to version control.
