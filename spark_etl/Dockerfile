# Dockerfile for Spark ETL Jobs
FROM apache/spark-py:v3.5.0

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Install AWS SDK for S3 access
RUN pip3 install --no-cache-dir \
    hadoop-aws==3.3.4 \
    aws-java-sdk-bundle==1.12.262

# Copy ETL job code
COPY jobs/ ./jobs/

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Configure Spark for S3
ENV SPARK_HADOOP_JAR=/opt/spark/jars/hadoop-aws-3.3.4.jar
ENV SPARK_AWS_JAR=/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Default command
CMD ["spark-submit", "--master", "local[*]", "--py-files", "/app/jobs", "/app/jobs/main.py"]

