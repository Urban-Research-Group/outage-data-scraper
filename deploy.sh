#!/bin/bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 904202461292.dkr.ecr.us-east-1.amazonaws.com; 
docker build -t outage-data-scraper . ; 
docker tag outage-data-scraper:latest 904202461292.dkr.ecr.us-east-1.amazonaws.com/outage-data-scraper:latest ; 
docker push 904202461292.dkr.ecr.us-east-1.amazonaws.com/outage-data-scraper:latest;  
echo "Deployed outage-data-scraper to ECR"