# Python application for ingesting objects from GCS bucket to AWS s3 bucket.

This is a Python application built using Flask for ingesting objects from GCS to AWS s3.


## Installation

Import this project in any Python IDE.

Using [pip](https://pypi.org/project/pip/), install the depedencies listed in `requirements.txt` file by running following command:

```
pip install -r requirements.txt
```

#### Adding environment variables
```
Add the 'CLOUD_STORAGE_BUCKET_SOURCE' system variable with the source GCP bucket name.
Add the 'AWS_S3_BUCKET_DEST' system variable with the destination AWS S3 bucket bucket name.
Add the 'AWS_ACCESS_KEY_ID' system variable with appropriate AWS credential.
Add the 'AWS_SECRET_ACCESS_KEY' system variable with appropriate AWS credential.
Add the 'AWS_SESSION_TOKEN' system variable with appropriate AWS credential.
```

#### Running the project
```
python app.py
```

## Usage

For complete automated setup, a trigger needs to be set on GCS bucket to GCP PubSub.

#### Set a notification on the GCS bucket to trigger a message to GCP PubSub. Run the following command in GCloud SDK CLI
```
gsutil notification create -t user-chat-session-transfer -f json -e OBJECT_FINALIZE gs://lms_user_chat
```

#### Create a PUSH subscription

For the topic created above, create a PUSH subscription with the the below external endpoint:

This application exposes the following API endpoints: 

- Generate and view word cloud
 
API uri: `/transfer` method: `POST`
 


## Deploying the application in GCP CloudRun


Install and configure the [GCP SDK CLI](https://cloud.google.com/sdk) if not installed previously.

Modify the projectID and application name in the [cloudbuild.yaml](./cloudbuild.yaml) as per the requirement.

#### Add deployment permissions (Only if not done before)

1. Open the Cloud Build settings page in the Cloud Console:
(https://console.cloud.google.com/cloud-build/settings?_ga=2.259026890.1381264803.1595383748-320377619.1593537952)

2. In the Service account permissions panel, set the status of the Cloud Run Admin role to ENABLED:

#### Deploying the application using the automated script
Execute the following command to auto-deploy the application in CloudRun.
```
gcloud builds submit --config cloudbuild.yaml .
```


## Libraries used

1) [google-cloud-storage](https://pypi.org/project/google-cloud-storage/)
2) [boto3](https://pypi.org/project/boto3/)