#!/bin/bash
# ECS Cluster Setup Script
# Creates ECS cluster, task definitions, and service configurations

set -e

AWS_REGION=${AWS_REGION:-us-east-1}
CLUSTER_NAME=${CLUSTER_NAME:-starlink-etl-cluster}
PROJECT_NAME=${PROJECT_NAME:-starlink-etl}

echo "Setting up ECS cluster: $CLUSTER_NAME in region: $AWS_REGION"

# Create ECS cluster
echo "Creating ECS cluster..."
aws ecs create-cluster \
    --cluster-name "$CLUSTER_NAME" \
    --region "$AWS_REGION" || \
    echo "Cluster may already exist"

# Create CloudWatch log group
echo "Creating CloudWatch log group..."
aws logs create-log-group \
    --log-group-name "/ecs/$PROJECT_NAME" \
    --region "$AWS_REGION" || \
    echo "Log group may already exist"

# Register task definition (requires manual update of account ID and ECR repo)
echo "Note: Update ecs-task-definition.json with your account ID and ECR repository URL"
echo "Then run: aws ecs register-task-definition --cli-input-json file://ecs-task-definition.json --region $AWS_REGION"

echo "ECS cluster setup initiated!"
echo "Next steps:"
echo "1. Update ecs-task-definition.json with your AWS account ID and ECR repository"
echo "2. Register the task definition"
echo "3. Create ECS service or run tasks manually"

