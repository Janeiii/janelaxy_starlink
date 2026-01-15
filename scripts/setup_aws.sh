#!/bin/bash
# AWS Infrastructure Setup Script
# Sets up all AWS resources needed for the Starlink ETL pipeline

set -e

AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME=${PROJECT_NAME:-starlink-etl}

echo "Setting up AWS infrastructure for Starlink ETL Pipeline..."
echo "Region: $AWS_REGION"

# Check AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "Error: AWS credentials are not configured. Run 'aws configure' first."
    exit 1
fi

echo "AWS credentials verified."

# Setup S3 buckets
echo ""
echo "=== Setting up S3 buckets ==="
cd infrastructure
bash s3_setup.sh
cd ..

# Setup RDS (manual step - provide instructions)
echo ""
echo "=== RDS Setup ==="
echo "To set up RDS PostgreSQL:"
echo "1. Go to AWS RDS Console"
echo "2. Create a PostgreSQL database instance"
echo "3. Note the endpoint, port, username, and password"
echo "4. Run the SQL script: infrastructure/rds_setup.sql"
echo "5. Update environment variables with RDS connection details"

# Setup ECS cluster
echo ""
echo "=== Setting up ECS cluster ==="
cd infrastructure
bash ecs_cluster_setup.sh
cd ..

# Create IAM roles (instructions)
echo ""
echo "=== IAM Roles Setup ==="
echo "Create the following IAM roles:"
echo "1. ECS Task Execution Role (for pulling images from ECR)"
echo "2. ECS Task Role (for accessing S3, RDS) - use infrastructure/iam_policy.json"
echo ""
echo "Update infrastructure/ecs-task-definition.json with the role ARNs"

echo ""
echo "AWS infrastructure setup initiated!"
echo "Please complete the manual steps above."

