-- RDS PostgreSQL Schema Setup for Starlink ETL Pipeline
-- Run this script to create the necessary tables for aggregated metrics

-- Create database (run as superuser)
-- CREATE DATABASE starlink;

-- Connect to starlink database
-- \c starlink;

-- Satellite Statistics Table
CREATE TABLE IF NOT EXISTS satellite_statistics (
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    total_satellites INTEGER,
    operational_count INTEGER,
    maintenance_count INTEGER,
    standby_count INTEGER,
    avg_altitude NUMERIC(10, 2),
    avg_velocity NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (time_window_start, time_window_end)
);

-- Coverage Statistics Table
CREATE TABLE IF NOT EXISTS coverage_statistics (
    region_id VARCHAR(10),
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    avg_coverage_percentage NUMERIC(5, 2),
    max_coverage_percentage NUMERIC(5, 2),
    min_coverage_percentage NUMERIC(5, 2),
    avg_satellite_count NUMERIC(10, 2),
    max_satellite_count INTEGER,
    avg_signal_strength NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (region_id, time_window_start, time_window_end)
);

-- Performance Statistics Table
CREATE TABLE IF NOT EXISTS performance_statistics (
    region VARCHAR(10),
    time_window_start TIMESTAMP,
    time_window_end TIMESTAMP,
    avg_latency_ms NUMERIC(10, 2),
    min_latency_ms NUMERIC(10, 2),
    max_latency_ms NUMERIC(10, 2),
    avg_throughput_mbps NUMERIC(10, 2),
    min_throughput_mbps NUMERIC(10, 2),
    max_throughput_mbps NUMERIC(10, 2),
    avg_availability_percentage NUMERIC(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (region, time_window_start, time_window_end)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_satellite_stats_time ON satellite_statistics(time_window_start);
CREATE INDEX IF NOT EXISTS idx_coverage_stats_region_time ON coverage_statistics(region_id, time_window_start);
CREATE INDEX IF NOT EXISTS idx_performance_stats_region_time ON performance_statistics(region, time_window_start);

-- Create view for dashboard queries
CREATE OR REPLACE VIEW dashboard_summary AS
SELECT 
    cs.region_id,
    cs.time_window_start,
    cs.avg_coverage_percentage,
    cs.avg_satellite_count,
    ps.avg_latency_ms,
    ps.avg_throughput_mbps,
    ps.avg_availability_percentage
FROM coverage_statistics cs
LEFT JOIN performance_statistics ps 
    ON cs.region_id = ps.region 
    AND cs.time_window_start = ps.time_window_start;

