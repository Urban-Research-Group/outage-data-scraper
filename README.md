# Outage Data Scraper & AWS Lambda Deployment

Web scraper gathering GA, CA, TX, IL, NY, TN raw power outage data and unifying schemas for research and analysis.

## Source code

The src code for this project is located in the [app](./app) directory; [main.py](./app/main.py)
contains the lambda function handler and modules [scraper](./app/scrapers).

## Environment

If you do not have a python distribution installed yet, we recommend installing Anaconda: https://www.ana-conda.com/ (or miniconda) with Python 3.

You can use `env.yaml` and `requirements.txt` to create a copy of conda environment.

`conda env create -f env.yaml`
`conda activate urg`
`pip3 install -r requirements.txt`

Refer to the users’ manual: https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html for more details.

After you finish your task, you can leave this environment by `conda deactivate`.

## Local test

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

For local test, please use ChromeDriverManager (in GA_scraper)

```
driver = webdriver.Chrome(
            ChromeDriverManager().install(),  # for local test
            # executable_path=chrome_driver_path,
            chrome_options=chrome_options,
            seleniumwire_options=selenium_options,
            desired_capabilities=desired_capabilities,
        )
```

Remember to comment out ChromeDriver (and also import) and use executable_path=chrome_driver_path before deploying to AWS lambda

## Lambda deployment

Push the above built image to the AWS Elastic Container Registry (ECR) and create the Lambda function based on the image. Refer to [official doc](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-images.html)

Please refer to assignment 2's slide for more details

## Docker build

We use Amazon ECR to deploy out lambda function, so please use the instruction to build and push the docker image.

The [Dockerfile](./Dockerfile) contains the instructions to build this image. Run the command to create the image outage-data-scraper tagged latest.

```
docker build -t outage-data-scraper:latest .
```

Contributors:

Rickon,

Chi-Ting,

Rakshith
