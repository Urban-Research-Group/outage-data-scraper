# Define global args
ARG FUNCTION_DIR="/home/app"
ARG RUNTIME_VERSION="3.10"

# Stage 1 - bundle base image + runtime
FROM python:${RUNTIME_VERSION}-buster as build-image
RUN apt-get update && \
 apt-get install -y \
    g++ \
    make \
    cmake \
    unzip \
    libcurl4-openssl-dev

# Stage 2 - build function and dependencies
# Include global args in this stage of the build
ARG FUNCTION_DIR
# Create function directory
RUN mkdir -p ${FUNCTION_DIR}
# Copy handler function
COPY app/ ${FUNCTION_DIR}

# Optional – Install the function's dependencies
WORKDIR ${FUNCTION_DIR}
# Install Lambda Runtime Interface Client for Python
RUN python${RUNTIME_VERSION} -m pip install awslambdaric --target ${FUNCTION_DIR}

# Stage 3 - final runtime image
# Grab a fresh copy of the Python image
FROM python:3.10-buster
# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}
# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}
#RUN apk add chromium chromium-chromedriver
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache \
    pip install -r requirements.txt --target ${FUNCTION_DIR}

# (Optional) Add Lambda Runtime Interface Emulator and use a script in the ENTRYPOINT for simpler local runs
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/bin/aws-lambda-rie
COPY entry.sh /
RUN chmod 755 /usr/bin/aws-lambda-rie /entry.sh

ENV APP_VERSION=1.0.0
ENTRYPOINT [ "/entry.sh" ]
CMD [ "main.handler" ]


