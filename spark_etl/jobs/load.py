"""
Spark ETL Load Module
Writes processed data to S3 (Parquet) and loads aggregated metrics to RDS/DynamoDB
"""

from pyspark.sql import SparkSession, DataFrame
import logging
import psycopg2
from psycopg2.extras import execute_values
from typing import Optional, Dict
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """Loads processed data to S3 and databases"""
    
    def __init__(self, spark: SparkSession, s3_bucket: str, 
                 s3_endpoint: str = None, aws_region: str = "us-east-1"):
        """
        Initialize DataLoader
        
        Args:
            spark: SparkSession instance
            s3_bucket: S3 bucket name for processed data
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
    
    def write_to_s3_parquet(self, df: DataFrame, s3_path: str, 
                           partition_by: Optional[list] = None,
                           mode: str = "overwrite") -> bool:
        """
        Write DataFrame to S3 as Parquet format
        
        Args:
            df: DataFrame to write
            s3_path: S3 path (relative to bucket root)
            partition_by: List of columns to partition by
            mode: Write mode ('overwrite', 'append', 'ignore', 'error')
            
        Returns:
            True if successful, False otherwise
        """
        if df is None:
            logger.error("DataFrame is None, cannot write to S3")
            return False
        
        try:
            full_path = f"s3a://{self.s3_bucket}/{s3_path}"
            logger.info(f"Writing to S3: {full_path}")
            
            writer = df.write.mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.parquet(full_path)
            
            row_count = df.count()
            logger.info(f"Successfully wrote {row_count} rows to {s3_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}")
            return False
    
    def get_rds_connection(self, host: str, port: int, database: str,
                          user: str, password: str):
        """Get PostgreSQL connection"""
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            return conn
        except Exception as e:
            logger.error(f"Error connecting to RDS: {str(e)}")
            return None
    
    def load_to_rds(self, df: DataFrame, table_name: str,
                    rds_config: Dict) -> bool:
        """
        Load aggregated metrics to RDS PostgreSQL
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            rds_config: Dictionary with RDS connection config
                       (host, port, database, user, password)
            
        Returns:
            True if successful, False otherwise
        """
        if df is None:
            logger.error("DataFrame is None, cannot load to RDS")
            return False
        
        try:
            logger.info(f"Loading data to RDS table: {table_name}")
            
            # Convert Spark DataFrame to Pandas for easier database insertion
            pandas_df = df.toPandas()
            
            if len(pandas_df) == 0:
                logger.warning("No data to load to RDS")
                return True
            
            # Get database connection
            conn = self.get_rds_connection(**rds_config)
            if not conn:
                return False
            
            cursor = conn.cursor()
            
            # Get column names
            columns = list(pandas_df.columns)
            
            # Create table if it doesn't exist (simplified schema)
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f"{col} TEXT" for col in columns])}
            )
            """
            cursor.execute(create_table_sql)
            conn.commit()
            
            # Insert data
            values = [tuple(row) for row in pandas_df.values]
            insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES %s
            ON CONFLICT DO NOTHING
            """
            
            execute_values(cursor, insert_sql, values)
            conn.commit()
            
            cursor.close()
            conn.close()
            
            logger.info(f"Successfully loaded {len(pandas_df)} rows to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading to RDS: {str(e)}")
            return False
    
    def load_all_data(self, data_dict: Dict[str, DataFrame],
                     s3_base_path: str = "processed",
                     rds_config: Optional[Dict] = None) -> bool:
        """
        Load all processed data to S3 and optionally to RDS
        
        Args:
            data_dict: Dictionary with DataFrames to load
            s3_base_path: Base path in S3 for processed data
            rds_config: Optional RDS configuration
            
        Returns:
            True if all loads successful
        """
        success = True
        
        # Load satellite data to S3
        if 'satellite' in data_dict and data_dict['satellite'] is not None:
            sat_path = f"{s3_base_path}/satellite_data"
            if not self.write_to_s3_parquet(
                data_dict['satellite'], 
                sat_path,
                partition_by=["year", "month", "day"]
            ):
                success = False
        
        # Load coverage data to S3
        if 'coverage' in data_dict and data_dict['coverage'] is not None:
            cov_path = f"{s3_base_path}/coverage_data"
            if not self.write_to_s3_parquet(
                data_dict['coverage'],
                cov_path,
                partition_by=["region_id"]
            ):
                success = False
        
        # Load performance data to S3
        if 'performance' in data_dict and data_dict['performance'] is not None:
            perf_path = f"{s3_base_path}/performance_metrics"
            if not self.write_to_s3_parquet(
                data_dict['performance'],
                perf_path,
                partition_by=["region"]
            ):
                success = False
        
        # Load aggregated statistics to S3 and RDS
        if 'satellite_stats' in data_dict and data_dict['satellite_stats'] is not None:
            stats_path = f"{s3_base_path}/satellite_statistics"
            if not self.write_to_s3_parquet(
                data_dict['satellite_stats'],
                stats_path
            ):
                success = False
            
            # Load to RDS if configured
            if rds_config:
                if not self.load_to_rds(
                    data_dict['satellite_stats'],
                    "satellite_statistics",
                    rds_config
                ):
                    success = False
        
        if 'coverage_stats' in data_dict and data_dict['coverage_stats'] is not None:
            stats_path = f"{s3_base_path}/coverage_statistics"
            if not self.write_to_s3_parquet(
                data_dict['coverage_stats'],
                stats_path
            ):
                success = False
            
            if rds_config:
                if not self.load_to_rds(
                    data_dict['coverage_stats'],
                    "coverage_statistics",
                    rds_config
                ):
                    success = False
        
        if 'performance_stats' in data_dict and data_dict['performance_stats'] is not None:
            stats_path = f"{s3_base_path}/performance_statistics"
            if not self.write_to_s3_parquet(
                data_dict['performance_stats'],
                stats_path
            ):
                success = False
            
            if rds_config:
                if not self.load_to_rds(
                    data_dict['performance_stats'],
                    "performance_statistics",
                    rds_config
                ):
                    success = False
        
        return success

