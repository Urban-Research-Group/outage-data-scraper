
# Use pre-built selenium-python base image
FROM umihico/aws-lambda-selenium-python:latest

# copy scraper code
COPY app/ ${LAMBDA_TASK_ROOT}

# install dependencies
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache \
    pip3 install -r requirements.txt "${LAMBDA_TASK_ROOT}"

ENV APP_VERSION=1.0.0

CMD ["main.handler"]
