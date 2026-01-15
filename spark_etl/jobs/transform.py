"""
Spark ETL Transform Module
Data cleaning, aggregations, orbital calculations, and coverage statistics
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, avg, max as spark_max, min as spark_min,
    sum as spark_sum, round, hour, dayofmonth, month, year,
    window, from_unixtime, unix_timestamp, lit, abs as spark_abs
)
from pyspark.sql.types import DoubleType
import logging
from typing import Optional, Dict
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms and cleans Starlink satellite data"""
    
    def __init__(self, spark: SparkSession):
        """Initialize DataTransformer"""
        self.spark = spark
    
    def clean_satellite_data(self, df: DataFrame) -> Optional[DataFrame]:
        """
        Clean and validate satellite data
        
        - Remove null satellite_ids
        - Validate coordinate ranges
        - Fill missing values
        - Normalize status values
        """
        if df is None:
            logger.error("Input DataFrame is None")
            return None
        
        try:
            logger.info("Cleaning satellite data...")
            
            # Remove rows with null satellite_id
            df_clean = df.filter(col("satellite_id").isNotNull())
            
            # Validate latitude range (-90 to 90)
            df_clean = df_clean.filter(
                (col("latitude") >= -90) & (col("latitude") <= 90)
            )
            
            # Validate longitude range (-180 to 180)
            df_clean = df_clean.filter(
                (col("longitude") >= -180) & (col("longitude") <= 180)
            )
            
            # Validate altitude (should be positive, typically 500-600 km)
            df_clean = df_clean.filter(
                (col("altitude") > 0) & (col("altitude") < 2000)
            )
            
            # Normalize status values
            df_clean = df_clean.withColumn(
                "status",
                when(col("status").isNull(), "unknown")
                .when(col("status").isin(["operational", "active", "online"]), "operational")
                .when(col("status").isin(["maintenance", "offline", "inactive"]), "maintenance")
                .when(col("status").isin(["standby", "idle"]), "standby")
                .otherwise("unknown")
            )
            
            # Fill missing velocity with average (if needed)
            avg_velocity = df_clean.agg(avg("velocity")).collect()[0][0]
            if avg_velocity:
                df_clean = df_clean.fillna({"velocity": avg_velocity})
            
            row_count = df_clean.count()
            logger.info(f"Cleaned satellite data: {row_count} rows")
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Error cleaning satellite data: {str(e)}")
            return None
    
    def calculate_orbital_metrics(self, df: DataFrame) -> Optional[DataFrame]:
        """
        Calculate additional orbital metrics
        
        - Orbital period estimates
        - Ground track velocity
        - Coverage area estimates
        """
        if df is None:
            return None
        
        try:
            logger.info("Calculating orbital metrics...")
            
            # Earth radius in km
            earth_radius_km = 6371.0
            
            # Calculate orbital period (simplified: T = 2π * sqrt(r³/GM))
            # For 550km altitude, period is approximately 95 minutes
            df_with_metrics = df.withColumn(
                "orbital_period_minutes",
                lit(95.0)  # Simplified constant
            )
            
            # Calculate ground track velocity (km/h)
            # Simplified: v = 2π * (R + h) / T
            df_with_metrics = df_with_metrics.withColumn(
                "ground_track_velocity_kmh",
                round((2 * math.pi * (earth_radius_km + col("altitude")) / 
                       col("orbital_period_minutes")) * 60, 2)
            )
            
            # Estimate coverage radius (simplified: based on altitude and viewing angle)
            # Typical Starlink coverage radius ~500-1000km
            df_with_metrics = df_with_metrics.withColumn(
                "estimated_coverage_radius_km",
                round(col("altitude") * 1.5, 2)
            )
            
            logger.info("Orbital metrics calculated")
            return df_with_metrics
            
        except Exception as e:
            logger.error(f"Error calculating orbital metrics: {str(e)}")
            return df
    
    def aggregate_coverage_statistics(self, df: DataFrame) -> Optional[DataFrame]:
        """
        Aggregate coverage statistics by region and time window
        
        Returns aggregated DataFrame with:
        - region_id
        - time_window
        - avg_coverage_percentage
        - max_coverage_percentage
        - min_coverage_percentage
        - avg_satellite_count
        - avg_signal_strength
        """
        if df is None:
            return None
        
        try:
            logger.info("Aggregating coverage statistics...")
            
            # Create time windows (hourly aggregation)
            df_with_window = df.withColumn(
                "time_window",
                window(col("timestamp"), "1 hour")
            )
            
            # Aggregate by region and time window
            aggregated = df_with_window.groupBy("region_id", "time_window") \
                .agg(
                    avg("coverage_percentage").alias("avg_coverage_percentage"),
                    spark_max("coverage_percentage").alias("max_coverage_percentage"),
                    spark_min("coverage_percentage").alias("min_coverage_percentage"),
                    avg("satellite_count").alias("avg_satellite_count"),
                    spark_max("satellite_count").alias("max_satellite_count"),
                    avg("avg_signal_strength").alias("avg_signal_strength")
                ) \
                .withColumn("avg_coverage_percentage", round(col("avg_coverage_percentage"), 2)) \
                .withColumn("avg_satellite_count", round(col("avg_satellite_count"), 2)) \
                .withColumn("avg_signal_strength", round(col("avg_signal_strength"), 2))
            
            logger.info("Coverage statistics aggregated")
            return aggregated
            
        except Exception as e:
            logger.error(f"Error aggregating coverage statistics: {str(e)}")
            return None
    
    def aggregate_performance_metrics(self, df: DataFrame) -> Optional[DataFrame]:
        """
        Aggregate performance metrics by region and time window
        
        Returns aggregated DataFrame with:
        - region
        - time_window
        - avg_latency_ms
        - avg_throughput_mbps
        - avg_availability_percentage
        """
        if df is None:
            return None
        
        try:
            logger.info("Aggregating performance metrics...")
            
            # Create time windows (hourly aggregation)
            df_with_window = df.withColumn(
                "time_window",
                window(col("timestamp"), "1 hour")
            )
            
            # Aggregate by region and time window
            aggregated = df_with_window.groupBy("region", "time_window") \
                .agg(
                    avg("latency_ms").alias("avg_latency_ms"),
                    spark_min("latency_ms").alias("min_latency_ms"),
                    spark_max("latency_ms").alias("max_latency_ms"),
                    avg("throughput_mbps").alias("avg_throughput_mbps"),
                    spark_min("throughput_mbps").alias("min_throughput_mbps"),
                    spark_max("throughput_mbps").alias("max_throughput_mbps"),
                    avg("availability_percentage").alias("avg_availability_percentage")
                ) \
                .withColumn("avg_latency_ms", round(col("avg_latency_ms"), 2)) \
                .withColumn("avg_throughput_mbps", round(col("avg_throughput_mbps"), 2)) \
                .withColumn("avg_availability_percentage", round(col("avg_availability_percentage"), 2))
            
            logger.info("Performance metrics aggregated")
            return aggregated
            
        except Exception as e:
            logger.error(f"Error aggregating performance metrics: {str(e)}")
            return None
    
    def aggregate_satellite_statistics(self, df: DataFrame) -> Optional[DataFrame]:
        """
        Aggregate satellite statistics by time window
        
        Returns aggregated DataFrame with:
        - time_window
        - total_satellites
        - operational_count
        - maintenance_count
        - avg_altitude
        - avg_velocity
        """
        if df is None:
            return None
        
        try:
            logger.info("Aggregating satellite statistics...")
            
            # Create time windows (hourly aggregation)
            df_with_window = df.withColumn(
                "time_window",
                window(col("timestamp"), "1 hour")
            )
            
            # Aggregate by time window
            aggregated = df_with_window.groupBy("time_window") \
                .agg(
                    count("*").alias("total_satellites"),
                    count(when(col("status") == "operational", 1)).alias("operational_count"),
                    count(when(col("status") == "maintenance", 1)).alias("maintenance_count"),
                    count(when(col("status") == "standby", 1)).alias("standby_count"),
                    avg("altitude").alias("avg_altitude"),
                    avg("velocity").alias("avg_velocity")
                ) \
                .withColumn("avg_altitude", round(col("avg_altitude"), 2)) \
                .withColumn("avg_velocity", round(col("avg_velocity"), 2))
            
            logger.info("Satellite statistics aggregated")
            return aggregated
            
        except Exception as e:
            logger.error(f"Error aggregating satellite statistics: {str(e)}")
            return None
    
    def join_datasets(self, satellite_df: DataFrame, coverage_df: DataFrame,
                     performance_df: DataFrame) -> Optional[DataFrame]:
        """
        Join satellite, coverage, and performance datasets
        
        Creates a comprehensive view of the data
        """
        try:
            logger.info("Joining datasets...")
            
            # Join coverage and performance on region and timestamp
            # First, align region identifiers
            coverage_aligned = coverage_df.withColumnRenamed("region_id", "region")
            
            # Join on region and time window
            joined = coverage_aligned.join(
                performance_df,
                (coverage_aligned.region == performance_df.region) &
                (coverage_aligned.timestamp == performance_df.timestamp),
                "outer"
            )
            
            logger.info("Datasets joined successfully")
            return joined
            
        except Exception as e:
            logger.error(f"Error joining datasets: {str(e)}")
            return None
    
    def transform_all(self, data_dict: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """
        Transform all datasets
        
        Args:
            data_dict: Dictionary with 'satellite', 'coverage', 'performance' DataFrames
            
        Returns:
            Dictionary with transformed DataFrames
        """
        results = {}
        
        # Transform satellite data
        if 'satellite' in data_dict and data_dict['satellite'] is not None:
            sat_clean = self.clean_satellite_data(data_dict['satellite'])
            if sat_clean:
                sat_with_metrics = self.calculate_orbital_metrics(sat_clean)
                results['satellite'] = sat_with_metrics
                results['satellite_stats'] = self.aggregate_satellite_statistics(sat_with_metrics)
        
        # Transform coverage data
        if 'coverage' in data_dict and data_dict['coverage'] is not None:
            results['coverage'] = data_dict['coverage']
            results['coverage_stats'] = self.aggregate_coverage_statistics(data_dict['coverage'])
        
        # Transform performance data
        if 'performance' in data_dict and data_dict['performance'] is not None:
            results['performance'] = data_dict['performance']
            results['performance_stats'] = self.aggregate_performance_metrics(data_dict['performance'])
        
        return results

