import os
import boto3
import io
import geopy

from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request


def is_aws_env():
    return os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or os.environ.get('AWS_EXECUTION_ENV')


def save(df, bucket_name=None, file_path=None):
    if is_aws_env():
        s3_client = boto3.client("s3")
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=bucket_name, Key=file_path, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
    else:
        local_path = f"{os.getcwd()}/../{bucket_name}/{file_path}"
        df.to_csv(local_path, index=False)
        print(f"outages data saved to {file_path}")

def check_duplicate():
    pass

def make_request(url, headers=None):
    # TODO: refactor all 'urlopen'
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/109.0.0.0 Safari/537.36'}
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=10) as response:
            print(response.status)
            return response.read(), response
    except HTTPError as error:
        print(error.status, error.reason)
    except URLError as error:
        print(error.reason)
    except TimeoutError:
        print("Request timed out")


def extract_zipcode(lat, lon, geo_locator):
    addr = geo_locator.reverse((lat, lon))
    if addr:
        return addr.raw['address'].get('postcode', 'unknown')
    else:
        return 'unknown'


def timenow():
    return datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")