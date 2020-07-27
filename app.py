import logging
import os

import boto3
from botocore.exceptions import ClientError
from flask import Flask, jsonify, request
from google.cloud import storage

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# Configure these environment variables
CLOUD_STORAGE_BUCKET_SOURCE = os.environ.get('CLOUD_STORAGE_BUCKET_SOURCE')
AWS_S3_BUCKET_DEST = os.environ.get('AWS_S3_BUCKET_DEST')


@app.route('/')
def index():
    return "welcome to object transfer app. This application transfers objects from GCS to S3 bucket"


def fetch_and_upload_data(bucket, object_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(object_name)

    filename = 'tmp_' + object_name
    blob.download_to_filename(filename)

    app.logger.info("downloaded file {}".format(filename))

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(filename, AWS_S3_BUCKET_DEST, object_name)
        app.logger.info("response from s3 client: {}".format(response))
    except ClientError as e:
        logging.error(e)
        return False

    if os.path.exists(filename):
        os.remove(filename)

    app.logger.info("uploaded file: {} to bucket {}".format(filename, AWS_S3_BUCKET_DEST))
    return jsonify({'message': 'uploaded file: {} to bucket {}'.format(filename, AWS_S3_BUCKET_DEST)})


@app.route('/transfer', methods=['POST'])
def upload():
    data = request.get_json()
    app.logger.info(data)

    message = data['message']
    attributes = message['attributes']

    bucket = attributes['bucketId']
    object_name = attributes['objectId']

    resp = fetch_and_upload_data(bucket, object_name)
    return resp

    return jsonify(message=data)


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
