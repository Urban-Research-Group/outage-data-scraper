# Outage Data Scraper & AWS Lambda Deployment 
Web scraper to gather 36 EMCs (Electric Membership Corporation) raw power outage data and parse into .csv format. Then save to S3 buckets for further data pipeline and analysis.
https://docs.google.com/spreadsheets/d/1sknQle2RQAFSRId9O5JyoNozgHtig1Rx6u2pnippaMQ/edit#gid=0


## Source code
The src code for this project is located in the [app](./app) directory; [main.py](./app/main.py)
contains the lambda function handler and modules [scraper](./app/scraper).

## Docker build
The [Dockerfile](./Dockerfile) contains the instructions to build this image. Run the command to create the image outage-data-scraper tagged latest.
```
docker build -t outage-data-scraper:latest .
```
## Local test
he image can be run locally before you deploy to deploy to AWS Lambda. Providing
AWS credentials is set up in `~/.aws/credentials`. Simply run the
command.
```
docker run -p 9000:8080 -v ~/.aws/:/root/.aws/ outage-data-scraper:latest
```
The -v flag mounts your local AWS credentials into the docker container allowing it access
to your AWS account and S3 bucket.

To confirm the container is working as it should locally, you can run a similar command to
```
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '<test_event>'
```

## Lambda deployment
AWS Lambda can now take an image to run as a serverless function. At the time of
writing this feature is limited to certain regions so you will need to carefully select
the region. For extra help, AWS have published a guide to working with containers in 
Lambda https://docs.aws.amazon.com/lambda/latest/dg/lambda-images.html.

You will also need to create an Elastic Container Registry within AWS - a place to 
store your Docker images so they can be used by Lambda. For extra help, AWS have published
a guide to working with ECR https://docs.aws.amazon.com/AmazonECR/latest/userguide/getting-started-cli.html.
        



Contributors:
Rickon, 
Rakshith
