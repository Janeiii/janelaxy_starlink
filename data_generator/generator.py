"""
Main data generator script for Starlink ETL pipeline
Generates simulated satellite data in various formats
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from satellite_simulator import SatelliteSimulator

try:
    import boto3
    from botocore.client import Config
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


def generate_data(output_dir: str, format: str = 'csv', 
                 num_satellites: int = 100, 
                 hours: int = 24,
                 frequency_minutes: int = 5):
    """
    Generate Starlink satellite data
    
    Args:
        output_dir: Directory to save generated data
        format: Output format ('csv', 'json', 'parquet')
        num_satellites: Number of satellites to simulate
        hours: Number of hours of data to generate
        frequency_minutes: Data generation frequency in minutes
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    simulator = SatelliteSimulator(num_satellites=num_satellites)
    
    # Generate timestamps
    start_time = datetime.now().replace(minute=0, second=0, microsecond=0)
    timestamps = []
    current_time = start_time
    end_time = start_time + timedelta(hours=hours)
    
    while current_time <= end_time:
        timestamps.append(current_time)
        current_time += timedelta(minutes=frequency_minutes)
    
    print(f"Generating data for {len(timestamps)} timestamps...")
    
    # Generate satellite data
    satellite_data_list = []
    coverage_data_list = []
    performance_data_list = []
    
    for i, timestamp in enumerate(timestamps):
        if (i + 1) % 10 == 0:
            print(f"Processing timestamp {i+1}/{len(timestamps)}: {timestamp}")
        
        # Generate satellite positions
        sat_df = simulator.generate_satellite_data(timestamp)
        satellite_data_list.append(sat_df)
        
        # Generate coverage data
        coverage_df = simulator.generate_coverage_data(timestamp)
        coverage_data_list.append(coverage_df)
        
        # Generate performance metrics
        perf_df = simulator.generate_performance_metrics(timestamp)
        performance_data_list.append(perf_df)
    
    # Combine all dataframes
    satellite_df = pd.concat(satellite_data_list, ignore_index=True)
    coverage_df = pd.concat(coverage_data_list, ignore_index=True)
    performance_df = pd.concat(performance_data_list, ignore_index=True)
    
    # Save data in requested format
    if format.lower() == 'csv':
        satellite_df.to_csv(output_path / 'satellite_data.csv', index=False)
        coverage_df.to_csv(output_path / 'coverage_data.csv', index=False)
        performance_df.to_csv(output_path / 'performance_metrics.csv', index=False)
        print(f"CSV files saved to {output_path}")
    
    elif format.lower() == 'json':
        satellite_df.to_json(output_path / 'satellite_data.json', orient='records', date_format='iso')
        coverage_df.to_json(output_path / 'coverage_data.json', orient='records', date_format='iso')
        performance_df.to_json(output_path / 'performance_metrics.json', orient='records', date_format='iso')
        print(f"JSON files saved to {output_path}")
    
    elif format.lower() == 'parquet':
        # Save as Parquet (partitioned by date if needed)
        pq.write_table(pa.Table.from_pandas(satellite_df), 
                      output_path / 'satellite_data.parquet')
        pq.write_table(pa.Table.from_pandas(coverage_df), 
                      output_path / 'coverage_data.parquet')
        pq.write_table(pa.Table.from_pandas(performance_df), 
                      output_path / 'performance_metrics.parquet')
        print(f"Parquet files saved to {output_path}")
    
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    print(f"\nData generation complete!")
    print(f"  - Satellite records: {len(satellite_df)}")
    print(f"  - Coverage records: {len(coverage_df)}")
    print(f"  - Performance records: {len(performance_df)}")
    
    return output_path, satellite_df, coverage_df, performance_df


def upload_to_minio(output_path: Path, bucket_name: str, 
                    endpoint_url: str = None, 
                    access_key: str = None, 
                    secret_key: str = None):
    """
    Upload generated files to MinIO (S3-compatible storage)
    
    Args:
        output_path: Path to directory containing generated files
        bucket_name: MinIO bucket name
        endpoint_url: MinIO endpoint URL (e.g., http://minio:9000)
        access_key: MinIO access key
        secret_key: MinIO secret key
    """
    if not BOTO3_AVAILABLE:
        print("Warning: boto3 not available, skipping MinIO upload")
        return
    
    # Get credentials from environment or parameters
    endpoint_url = endpoint_url or os.getenv('S3_ENDPOINT_URL') or os.getenv('MINIO_ENDPOINT')
    access_key = access_key or os.getenv('MINIO_ACCESS_KEY') or os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    secret_key = secret_key or os.getenv('MINIO_SECRET_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    
    if not endpoint_url:
        print("No MinIO endpoint specified, skipping upload")
        return
    
    try:
        # Create S3 client with MinIO endpoint
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")
        
        # Upload files
        files_to_upload = [
            'satellite_data.csv',
            'coverage_data.csv',
            'performance_metrics.csv',
            'satellite_data.json',
            'coverage_data.json',
            'performance_metrics.json',
            'satellite_data.parquet',
            'coverage_data.parquet',
            'performance_metrics.parquet'
        ]
        
        uploaded_count = 0
        for filename in files_to_upload:
            file_path = output_path / filename
            if file_path.exists():
                s3_client.upload_file(str(file_path), bucket_name, filename)
                print(f"Uploaded: {filename}")
                uploaded_count += 1
        
        print(f"Successfully uploaded {uploaded_count} files to MinIO bucket: {bucket_name}")
        
    except Exception as e:
        print(f"Error uploading to MinIO: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='Generate simulated Starlink satellite data')
    parser.add_argument('--output-dir', '-o', type=str, default='./data',
                      help='Output directory for generated data')
    parser.add_argument('--format', '-f', type=str, default='csv',
                      choices=['csv', 'json', 'parquet'],
                      help='Output format')
    parser.add_argument('--num-satellites', '-n', type=int, default=100,
                      help='Number of satellites to simulate')
    parser.add_argument('--hours', type=int, default=24,
                      help='Number of hours of data to generate')
    parser.add_argument('--frequency', type=int, default=5,
                      help='Data generation frequency in minutes')
    parser.add_argument('--upload-to-minio', action='store_true',
                      help='Upload generated files to MinIO')
    parser.add_argument('--minio-bucket', type=str, default=None,
                      help='MinIO bucket name for upload')
    
    args = parser.parse_args()
    
    try:
        output_path, sat_df, cov_df, perf_df = generate_data(
            output_dir=args.output_dir,
            format=args.format,
            num_satellites=args.num_satellites,
            hours=args.hours,
            frequency_minutes=args.frequency
        )
        
        # Upload to MinIO if requested
        if args.upload_to_minio:
            bucket_name = args.minio_bucket or os.getenv('S3_BUCKET', 'starlink-etl-raw-data')
            upload_to_minio(output_path, bucket_name)
            
    except Exception as e:
        print(f"Error generating data: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

