# How to Run 
## Step 1: Log in to ECR
`aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 331306402361.dkr.ecr.us-east-1.amazonaws.com`

## Step 2: Build Image
`docker build -t 331306402361.dkr.ecr.us-east-1.amazonaws.com/building-trace:latest .`

## Step 3: Push Image
`docker push 331306402361.dkr.ecr.us-east-1.amazonaws.com/building-trace:latest`

# Notes:
This repository will be transferred to cmi and deployed on Kubernetes. 
Some parts of the code might be rewritten for Kubernetes.