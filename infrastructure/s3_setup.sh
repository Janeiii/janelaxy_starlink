#!/bin/bash
# S3 Bucket Setup Script
# Creates S3 buckets for raw data, processed data, and Spark checkpoints

set -e

AWS_REGION=${AWS_REGION:-us-east-1}
PROJECT_NAME=${PROJECT_NAME:-starlink-etl}

RAW_BUCKET="${PROJECT_NAME}-raw-data"
PROCESSED_BUCKET="${PROJECT_NAME}-processed"
CHECKPOINT_BUCKET="${PROJECT_NAME}-checkpoints"

echo "Creating S3 buckets in region: $AWS_REGION"

# Create raw data bucket
echo "Creating raw data bucket: $RAW_BUCKET"
aws s3api create-bucket \
    --bucket "$RAW_BUCKET" \
    --region "$AWS_REGION" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION" || \
    echo "Bucket $RAW_BUCKET may already exist"

# Create processed data bucket
echo "Creating processed data bucket: $PROCESSED_BUCKET"
aws s3api create-bucket \
    --bucket "$PROCESSED_BUCKET" \
    --region "$AWS_REGION" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION" || \
    echo "Bucket $PROCESSED_BUCKET may already exist"

# Create checkpoint bucket
echo "Creating checkpoint bucket: $CHECKPOINT_BUCKET"
aws s3api create-bucket \
    --bucket "$CHECKPOINT_BUCKET" \
    --region "$AWS_REGION" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION" || \
    echo "Bucket $CHECKPOINT_BUCKET may already exist"

# Enable versioning on all buckets
echo "Enabling versioning on buckets..."
aws s3api put-bucket-versioning \
    --bucket "$RAW_BUCKET" \
    --versioning-configuration Status=Enabled

aws s3api put-bucket-versioning \
    --bucket "$PROCESSED_BUCKET" \
    --versioning-configuration Status=Enabled

aws s3api put-bucket-versioning \
    --bucket "$CHECKPOINT_BUCKET" \
    --versioning-configuration Status=Enabled

# Set up lifecycle policies (optional - delete old versions after 90 days)
echo "Setting up lifecycle policies..."
cat > /tmp/lifecycle.json <<EOF
{
    "Rules": [
        {
            "Id": "DeleteOldVersions",
            "Status": "Enabled",
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 90
            }
        }
    ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
    --bucket "$RAW_BUCKET" \
    --lifecycle-configuration file:///tmp/lifecycle.json || true

aws s3api put-bucket-lifecycle-configuration \
    --bucket "$PROCESSED_BUCKET" \
    --lifecycle-configuration file:///tmp/lifecycle.json || true

rm /tmp/lifecycle.json

echo "S3 buckets created successfully!"
echo "Raw data bucket: $RAW_BUCKET"
echo "Processed data bucket: $PROCESSED_BUCKET"
echo "Checkpoint bucket: $CHECKPOINT_BUCKET"

