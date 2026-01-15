"""
Main ETL Job
Orchestrates the complete ETL pipeline: Extract -> Transform -> Load
"""

from pyspark.sql import SparkSession
from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader
import logging
import argparse
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "StarlinkETL") -> SparkSession:
    """Create and configure SparkSession"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_etl_job(s3_raw_bucket: str, s3_processed_bucket: str,
               s3_checkpoint_bucket: str, file_format: str = "csv",
               rds_config: dict = None, s3_endpoint: str = None):
    """
    Run the complete ETL pipeline
    
    Args:
        s3_raw_bucket: S3 bucket with raw data
        s3_processed_bucket: S3 bucket for processed data
        s3_checkpoint_bucket: S3 bucket for Spark checkpoints
        file_format: Input file format (csv, json, parquet)
        rds_config: Optional RDS configuration
        s3_endpoint: S3 endpoint URL (for MinIO, e.g., http://minio:9000)
    """
    spark = None
    try:
        # Create Spark session
        logger.info("Creating Spark session...")
        spark = create_spark_session()
        
        # Set checkpoint location
        checkpoint_path = f"s3a://{s3_checkpoint_bucket}/checkpoints"
        spark.sparkContext.setCheckpointDir(checkpoint_path)
        
        # Extract phase
        logger.info("=== EXTRACT PHASE ===")
        extractor = DataExtractor(spark, s3_raw_bucket, s3_endpoint=s3_endpoint)
        raw_data = extractor.extract_all_data(file_format=file_format)
        
        # Validate extraction
        if not raw_data.get('satellite') or not raw_data.get('coverage') or not raw_data.get('performance'):
            logger.error("Failed to extract required data")
            return False
        
        logger.info("Extract phase completed successfully")
        
        # Transform phase
        logger.info("=== TRANSFORM PHASE ===")
        transformer = DataTransformer(spark)
        processed_data = transformer.transform_all(raw_data)
        
        if not processed_data:
            logger.error("Transform phase failed")
            return False
        
        logger.info("Transform phase completed successfully")
        
        # Load phase
        logger.info("=== LOAD PHASE ===")
        loader = DataLoader(spark, s3_processed_bucket, s3_endpoint=s3_endpoint)
        success = loader.load_all_data(processed_data, rds_config=rds_config)
        
        if success:
            logger.info("Load phase completed successfully")
            logger.info("=== ETL JOB COMPLETED SUCCESSFULLY ===")
        else:
            logger.error("Load phase had errors")
        
        return success
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}", exc_info=True)
        return False
        
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


def main():
    parser = argparse.ArgumentParser(description='Starlink ETL Pipeline')
    parser.add_argument('--s3-raw-bucket', type=str, required=True,
                       help='S3 bucket with raw data')
    parser.add_argument('--s3-processed-bucket', type=str, required=True,
                       help='S3 bucket for processed data')
    parser.add_argument('--s3-checkpoint-bucket', type=str, required=True,
                       help='S3 bucket for Spark checkpoints')
    parser.add_argument('--file-format', type=str, default='csv',
                       choices=['csv', 'json', 'parquet'],
                       help='Input file format')
    parser.add_argument('--rds-host', type=str, default=None,
                       help='RDS host (optional)')
    parser.add_argument('--rds-port', type=int, default=5432,
                       help='RDS port')
    parser.add_argument('--rds-database', type=str, default=None,
                       help='RDS database name')
    parser.add_argument('--rds-user', type=str, default=None,
                       help='RDS username')
    parser.add_argument('--rds-password', type=str, default=None,
                       help='RDS password (or use RDS_PASSWORD env var)')
    parser.add_argument('--s3-endpoint', type=str, default=None,
                       help='S3 endpoint URL (for MinIO, e.g., http://minio:9000)')
    
    args = parser.parse_args()
    
    # Get S3 endpoint from environment or argument
    s3_endpoint = args.s3_endpoint or os.getenv('S3_ENDPOINT_URL') or os.getenv('MINIO_ENDPOINT')
    
    # Build RDS config if provided
    rds_config = None
    if args.rds_host:
        rds_config = {
            'host': args.rds_host,
            'port': args.rds_port,
            'database': args.rds_database or 'starlink',
            'user': args.rds_user,
            'password': args.rds_password or os.getenv('RDS_PASSWORD', '')
        }
    
    success = run_etl_job(
        s3_raw_bucket=args.s3_raw_bucket,
        s3_processed_bucket=args.s3_processed_bucket,
        s3_checkpoint_bucket=args.s3_checkpoint_bucket,
        file_format=args.file_format,
        rds_config=rds_config,
        s3_endpoint=s3_endpoint
    )
    
    exit(0 if success else 1)


if __name__ == '__main__':
    main()

