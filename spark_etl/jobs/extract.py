"""
Spark ETL Extract Module
Reads data from S3 raw data bucket with error handling and schema validation
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType
)
from pyspark.sql.functions import col, to_timestamp, when
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts data from S3 with schema validation and error handling"""
    
    # Define schemas for different data types
    SATELLITE_SCHEMA = StructType([
        StructField("satellite_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("altitude", DoubleType(), nullable=True),
        StructField("velocity", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True)
    ])
    
    COVERAGE_SCHEMA = StructType([
        StructField("region_id", StringType(), nullable=False),
        StructField("region_name", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("coverage_percentage", DoubleType(), nullable=True),
        StructField("satellite_count", IntegerType(), nullable=True),
        StructField("avg_signal_strength", DoubleType(), nullable=True)
    ])
    
    PERFORMANCE_SCHEMA = StructType([
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("latency_ms", DoubleType(), nullable=True),
        StructField("throughput_mbps", DoubleType(), nullable=True),
        StructField("availability_percentage", DoubleType(), nullable=True)
    ])
    
    def __init__(self, spark: SparkSession, s3_bucket: str, 
                 s3_endpoint: str = None, aws_region: str = "us-east-1"):
        """
        Initialize DataExtractor
        
        Args:
            spark: SparkSession instance
            s3_bucket: S3 bucket name for raw data
            s3_endpoint: S3 endpoint URL (for MinIO, e.g., http://minio:9000)
            aws_region: AWS region (not used for MinIO)
        """
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        
        # Configure Spark for S3/MinIO access
        self.spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        
        # If endpoint is provided (MinIO), use it; otherwise use default AWS
        if s3_endpoint:
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.endpoint", s3_endpoint
            )
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.path.style.access", "true"
            )
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        else:
            self.spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )
    
    def read_csv_from_s3(self, s3_path: str, schema: StructType, 
                         header: bool = True) -> Optional:
        """
        Read CSV file from S3 with schema validation
        
        Args:
            s3_path: S3 path (relative to bucket root)
            schema: Expected schema
            header: Whether file has header row
            
        Returns:
            DataFrame or None if error
        """
        full_path = f"s3a://{self.s3_bucket}/{s3_path}"
        
        try:
            logger.info(f"Reading CSV from {full_path}")
            
            df = self.spark.read \
                .option("header", str(header).lower()) \
                .option("inferSchema", "false") \
                .schema(schema) \
                .csv(full_path)
            
            # Validate data
            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from {s3_path}")
            
            if row_count == 0:
                logger.warning(f"Empty file: {s3_path}")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading {s3_path}: {str(e)}")
            return None
    
    def read_json_from_s3(self, s3_path: str, schema: StructType) -> Optional:
        """
        Read JSON file from S3 with schema validation
        
        Args:
            s3_path: S3 path (relative to bucket root)
            schema: Expected schema
            
        Returns:
            DataFrame or None if error
        """
        full_path = f"s3a://{self.s3_bucket}/{s3_path}"
        
        try:
            logger.info(f"Reading JSON from {full_path}")
            
            df = self.spark.read \
                .option("multiline", "true") \
                .schema(schema) \
                .json(full_path)
            
            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from {s3_path}")
            
            if row_count == 0:
                logger.warning(f"Empty file: {s3_path}")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading {s3_path}: {str(e)}")
            return None
    
    def read_parquet_from_s3(self, s3_path: str) -> Optional:
        """
        Read Parquet file from S3
        
        Args:
            s3_path: S3 path (relative to bucket root)
            
        Returns:
            DataFrame or None if error
        """
        full_path = f"s3a://{self.s3_bucket}/{s3_path}"
        
        try:
            logger.info(f"Reading Parquet from {full_path}")
            
            df = self.spark.read.parquet(full_path)
            
            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from {s3_path}")
            
            if row_count == 0:
                logger.warning(f"Empty file: {s3_path}")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading {s3_path}: {str(e)}")
            return None
    
    def extract_satellite_data(self, file_path: str, file_format: str = "csv") -> Optional:
        """Extract satellite position data"""
        if file_format.lower() == "csv":
            return self.read_csv_from_s3(file_path, self.SATELLITE_SCHEMA)
        elif file_format.lower() == "json":
            return self.read_json_from_s3(file_path, self.SATELLITE_SCHEMA)
        elif file_format.lower() == "parquet":
            return self.read_parquet_from_s3(file_path)
        else:
            logger.error(f"Unsupported file format: {file_format}")
            return None
    
    def extract_coverage_data(self, file_path: str, file_format: str = "csv") -> Optional:
        """Extract coverage data"""
        if file_format.lower() == "csv":
            return self.read_csv_from_s3(file_path, self.COVERAGE_SCHEMA)
        elif file_format.lower() == "json":
            return self.read_json_from_s3(file_path, self.COVERAGE_SCHEMA)
        elif file_format.lower() == "parquet":
            return self.read_parquet_from_s3(file_path)
        else:
            logger.error(f"Unsupported file format: {file_format}")
            return None
    
    def extract_performance_data(self, file_path: str, file_format: str = "csv") -> Optional:
        """Extract performance metrics data"""
        if file_format.lower() == "csv":
            return self.read_csv_from_s3(file_path, self.PERFORMANCE_SCHEMA)
        elif file_format.lower() == "json":
            return self.read_json_from_s3(file_path, self.PERFORMANCE_SCHEMA)
        elif file_format.lower() == "parquet":
            return self.read_parquet_from_s3(file_path)
        else:
            logger.error(f"Unsupported file format: {file_format}")
            return None
    
    def extract_all_data(self, base_path: str = "", file_format: str = "csv") -> dict:
        """
        Extract all data types from S3
        
        Returns:
            Dictionary with 'satellite', 'coverage', 'performance' DataFrames
        """
        results = {}
        
        # Extract satellite data
        sat_path = f"{base_path}/satellite_data.{file_format}" if base_path else f"satellite_data.{file_format}"
        results['satellite'] = self.extract_satellite_data(sat_path, file_format)
        
        # Extract coverage data
        cov_path = f"{base_path}/coverage_data.{file_format}" if base_path else f"coverage_data.{file_format}"
        results['coverage'] = self.extract_coverage_data(cov_path, file_format)
        
        # Extract performance data
        perf_path = f"{base_path}/performance_metrics.{file_format}" if base_path else f"performance_metrics.{file_format}"
        results['performance'] = self.extract_performance_data(perf_path, file_format)
        
        return results

