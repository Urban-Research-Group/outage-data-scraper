# Outage Data Scraper & AWS Lambda Deployment 
Web scraper to gather GA, CA, TX raw power outage data and parse into .csv format. Then save to S3 buckets for further data pipeline and analysis. (Coveraged state expanding)
https://docs.google.com/spreadsheets/d/1sknQle2RQAFSRId9O5JyoNozgHtig1Rx6u2pnippaMQ/edit#gid=0


## Source code
The src code for this project is located in the [app](./app) directory; [main.py](./app/main.py)
contains the lambda function handler and modules [scraper](./app/scrapers).

## Docker build
The [Dockerfile](./Dockerfile) contains the instructions to build this image. Run the command to create the image outage-data-scraper tagged latest.
```
docker build -t outage-data-scraper:latest .
```
## Local test
he image can be run locally before you deploy to AWS Lambda. Providing
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
Push the above built image to the AWS Elastic Container Registry (ECR) and create the Lambda function based on the image. Refer to [official doc](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-images.html) 



Contributors:
Rickon, 
Rakshith
