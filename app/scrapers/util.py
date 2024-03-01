import os
import boto3
import io
import json
import csv
import requests
import pandas as pd

from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from urllib.parse import urlencode


def is_aws_env():
    return os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or os.environ.get(
        "AWS_EXECUTION_ENV"
    )


def save(df, bucket_name=None, file_path=None):
    if is_aws_env():
        # Check if the file exists in the bucket
        s3_client = boto3.client("s3")
        try:
            s3_client.head_object(Bucket=bucket_name, Key=file_path)
        except:
            # initial save
            s3_resource = boto3.resource("s3")
            s3_object = s3_resource.Object(bucket_name, file_path)
            response = s3_object.put(
                Body=df.to_csv(index=False), ContentType="text/csv"
            )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            print(f"new outages table initialized at {file_path}. Status - {status}")

        else:
            # combine and drop duplicate
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_path)
            df_og = pd.read_csv(io.BytesIO(s3_object["Body"].read()))
            df = pd.concat([df_og, df], ignore_index=True)
            # df.drop_duplicates(inplace=True)

            # update to s3
            with io.StringIO() as csv_buffer:
                df.to_csv(csv_buffer, index=False)

                response = s3_client.put_object(
                    Bucket=bucket_name, Key=file_path, Body=csv_buffer.getvalue()
                )
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            print(f"{bucket_name} outages updated to {file_path}. Status - {status}")

    else:
        # TODO check path
        local_path = f"{os.getcwd()}/{file_path}"
        header = False if os.path.exists(local_path) else True
        df.to_csv(local_path, mode="a", header=header, index=False)
        print(f"outages data saved to {file_path}")


def make_request(url, headers=None, data=None, method="GET"):
    # TODO: refactor all 'urlopen'
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/109.0.0.0 Safari/537.36"
        }

    if method == "GET":
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

    elif method == "POST":
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.text, response

    else:
        print("Invalid method")


def timenow():
    return datetime.strftime(datetime.now(), "%m-%d-%Y %H:%M:%S")
