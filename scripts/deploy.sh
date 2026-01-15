#!/bin/bash
# Deployment script for Starlink ETL Pipeline
# Handles building Docker images, pushing to ECR, and deploying to ECS

set -e

AWS_REGION=${AWS_REGION:-us-east-1}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-""}
ECR_REPO_PREFIX=${ECR_REPO_PREFIX:-starlink-etl}
CLUSTER_NAME=${CLUSTER_NAME:-starlink-etl-cluster}

if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "Error: AWS_ACCOUNT_ID environment variable must be set"
    exit 1
fi

echo "Deploying Starlink ETL Pipeline to AWS..."
echo "Region: $AWS_REGION"
echo "Account ID: $AWS_ACCOUNT_ID"

# Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Create ECR repositories if they don't exist
echo "Creating ECR repositories..."
for repo in spark-etl dashboard data-generator; do
    aws ecr describe-repositories --repository-names $ECR_REPO_PREFIX-$repo --region $AWS_REGION 2>/dev/null || \
    aws ecr create-repository --repository-name $ECR_REPO_PREFIX-$repo --region $AWS_REGION
done

# Build and push Spark ETL image
echo "Building and pushing Spark ETL image..."
cd spark_etl
docker build -t $ECR_REPO_PREFIX-spark-etl:latest .
docker tag $ECR_REPO_PREFIX-spark-etl:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_PREFIX-spark-etl:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_PREFIX-spark-etl:latest
cd ..

# Build and push Dashboard image
echo "Building and pushing Dashboard image..."
cd dashboard
docker build -t $ECR_REPO_PREFIX-dashboard:latest .
docker tag $ECR_REPO_PREFIX-dashboard:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_PREFIX-dashboard:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_PREFIX-dashboard:latest
cd ..

# Build and push Data Generator image
echo "Building and pushing Data Generator image..."
cd data_generator
docker build -t $ECR_REPO_PREFIX-data-generator:latest .
docker tag $ECR_REPO_PREFIX-data-generator:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_PREFIX-data-generator:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_PREFIX-data-generator:latest
cd ..

# Update ECS task definition with new image
echo "Updating ECS task definition..."
# Note: This requires manual update of the task definition JSON file
# with the correct image URLs before running this script

echo "Deployment complete!"
echo "Next steps:"
echo "1. Update infrastructure/ecs-task-definition.json with image URLs:"
echo "   $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_PREFIX-spark-etl:latest"
echo "2. Register the updated task definition:"
echo "   aws ecs register-task-definition --cli-input-json file://infrastructure/ecs-task-definition.json --region $AWS_REGION"
echo "3. Run the ETL task or create an ECS service"

