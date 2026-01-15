# Dockerfile for Data Generator
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install AWS CLI for S3 uploads
RUN pip install --no-cache-dir awscli boto3

# Copy application code
COPY . .

# Default command
CMD ["python", "generator.py", "--help"]

