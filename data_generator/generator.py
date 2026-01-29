"""
Main data generator script for Starlink ETL pipeline
Generates simulated satellite data in various formats
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from satellite_simulator import SatelliteSimulator


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
    
    args = parser.parse_args()
    
    try:
        output_path, sat_df, cov_df, perf_df = generate_data(
            output_dir=args.output_dir,
            format=args.format,
            num_satellites=args.num_satellites,
            hours=args.hours,
            frequency_minutes=args.frequency
        )
            
    except Exception as e:
        print(f"Error generating data: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

