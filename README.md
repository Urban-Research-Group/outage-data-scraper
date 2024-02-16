# Outage Data Scraper & AWS Lambda Deployment

Web scraper gathering GA, CA, TX, IL, NY, TN raw power outage data and unifying schemas for research and analysis.

## Source code

The src code for this project is located in the [app](./app) directory; [main.py](./app/main.py)
contains the lambda function handler and modules [scraper](./app/scrapers).

## Local test

he image can be run locally before you deploy to AWS Lambda. Providing
AWS credentials is set up in `~/.aws/credentials`. Simply run the
command.

Make a testing event looks like:

```
{
    "layout": 1,
    "emc": {
        "AEP Texas, Inc.": "http://outagemap.aeptexas.com.s3.amazonaws.com/external/report.html"
    },
    "bucket": "data",
    "folder": "tx"
}
```

Modify the path of testing event in `main.py` and you can execute the scrapers locally.

In the above testing event, we will use TXScraper's first scraper and store the result to `/data/tx/layout_1`. Please make the directory before running the program.

## Lambda deployment

Push the above built image to the AWS Elastic Container Registry (ECR) and create the Lambda function based on the image. Refer to [official doc](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-images.html)

## Docker build

We use Amazon ECR to deploy out lambda function, so please use the instruction to build and push the docker image.

The [Dockerfile](./Dockerfile) contains the instructions to build this image. Run the command to create the image outage-data-scraper tagged latest.

```
docker build -t outage-data-scraper:latest .
```

Contributors:
Rickon,
Chi-Ting
Rakshith
