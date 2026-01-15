"""
Flask Backend for Starlink Dashboard
Provides REST API endpoints for querying data from RDS and S3
"""

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import psycopg2
import boto3
import pandas as pd
import os
from datetime import datetime, timedelta
import json
from typing import Dict, Optional

app = Flask(__name__)
CORS(app)

# Configuration
RDS_CONFIG = {
    'host': os.getenv('RDS_HOST', 'localhost'),
    'port': int(os.getenv('RDS_PORT', 5432)),
    'database': os.getenv('RDS_DATABASE', 'starlink'),
    'user': os.getenv('RDS_USER', 'postgres'),
    'password': os.getenv('RDS_PASSWORD', 'postgres')
}

S3_CONFIG = {
    'bucket': os.getenv('S3_PROCESSED_BUCKET', 'starlink-etl-processed'),
    'endpoint_url': os.getenv('S3_ENDPOINT_URL') or os.getenv('MINIO_ENDPOINT'),
    'access_key': os.getenv('MINIO_ACCESS_KEY') or os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin'),
    'secret_key': os.getenv('MINIO_SECRET_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
}

# Initialize S3 client (works with both AWS S3 and MinIO)
if S3_CONFIG['endpoint_url']:
    # MinIO configuration
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_CONFIG['endpoint_url'],
        aws_access_key_id=S3_CONFIG['access_key'],
        aws_secret_access_key=S3_CONFIG['secret_key']
    )
else:
    # AWS S3 configuration (fallback)
    s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'us-east-1'))


def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(**RDS_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


@app.route('/')
def index():
    """Serve the main dashboard page"""
    return render_template('index.html')


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    db_status = "connected" if get_db_connection() else "disconnected"
    return jsonify({
        'status': 'healthy',
        'database': db_status,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/satellite/stats', methods=['GET'])
def get_satellite_stats():
    """Get satellite statistics"""
    hours = int(request.args.get('hours', 24))
    conn = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        query = """
            SELECT 
                time_window_start,
                total_satellites,
                operational_count,
                maintenance_count,
                standby_count,
                avg_altitude,
                avg_velocity
            FROM satellite_statistics
            WHERE time_window_start >= NOW() - INTERVAL '%s hours'
            ORDER BY time_window_start DESC
            LIMIT 100
        """
        cursor.execute(query, (hours,))
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/coverage/stats', methods=['GET'])
def get_coverage_stats():
    """Get coverage statistics by region"""
    region = request.args.get('region', None)
    hours = int(request.args.get('hours', 24))
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        if region:
            query = """
                SELECT 
                    region_id,
                    time_window_start,
                    avg_coverage_percentage,
                    max_coverage_percentage,
                    min_coverage_percentage,
                    avg_satellite_count,
                    avg_signal_strength
                FROM coverage_statistics
                WHERE region_id = %s 
                    AND time_window_start >= NOW() - INTERVAL '%s hours'
                ORDER BY time_window_start DESC
                LIMIT 100
            """
            cursor.execute(query, (region, hours))
        else:
            query = """
                SELECT 
                    region_id,
                    time_window_start,
                    avg_coverage_percentage,
                    max_coverage_percentage,
                    min_coverage_percentage,
                    avg_satellite_count,
                    avg_signal_strength
                FROM coverage_statistics
                WHERE time_window_start >= NOW() - INTERVAL '%s hours'
                ORDER BY region_id, time_window_start DESC
                LIMIT 500
            """
            cursor.execute(query, (hours,))
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/performance/stats', methods=['GET'])
def get_performance_stats():
    """Get performance metrics by region"""
    region = request.args.get('region', None)
    hours = int(request.args.get('hours', 24))
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        if region:
            query = """
                SELECT 
                    region,
                    time_window_start,
                    avg_latency_ms,
                    min_latency_ms,
                    max_latency_ms,
                    avg_throughput_mbps,
                    min_throughput_mbps,
                    max_throughput_mbps,
                    avg_availability_percentage
                FROM performance_statistics
                WHERE region = %s 
                    AND time_window_start >= NOW() - INTERVAL '%s hours'
                ORDER BY time_window_start DESC
                LIMIT 100
            """
            cursor.execute(query, (region, hours))
        else:
            query = """
                SELECT 
                    region,
                    time_window_start,
                    avg_latency_ms,
                    min_latency_ms,
                    max_latency_ms,
                    avg_throughput_mbps,
                    min_throughput_mbps,
                    max_throughput_mbps,
                    avg_availability_percentage
                FROM performance_statistics
                WHERE time_window_start >= NOW() - INTERVAL '%s hours'
                ORDER BY region, time_window_start DESC
                LIMIT 500
            """
            cursor.execute(query, (hours,))
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/summary', methods=['GET'])
def get_dashboard_summary():
    """Get summary data for dashboard"""
    hours = int(request.args.get('hours', 24))
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # Get latest satellite stats
        cursor.execute("""
            SELECT 
                SUM(total_satellites) as total,
                SUM(operational_count) as operational,
                AVG(avg_altitude) as avg_altitude
            FROM satellite_statistics
            WHERE time_window_start >= NOW() - INTERVAL '%s hours'
        """, (hours,))
        sat_stats = cursor.fetchone()
        
        # Get coverage by region
        cursor.execute("""
            SELECT 
                region_id,
                AVG(avg_coverage_percentage) as avg_coverage,
                AVG(avg_satellite_count) as avg_satellites
            FROM coverage_statistics
            WHERE time_window_start >= NOW() - INTERVAL '%s hours'
            GROUP BY region_id
        """, (hours,))
        coverage_data = cursor.fetchall()
        
        # Get performance summary
        cursor.execute("""
            SELECT 
                region,
                AVG(avg_latency_ms) as avg_latency,
                AVG(avg_throughput_mbps) as avg_throughput,
                AVG(avg_availability_percentage) as avg_availability
            FROM performance_statistics
            WHERE time_window_start >= NOW() - INTERVAL '%s hours'
            GROUP BY region
        """, (hours,))
        perf_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'satellite': {
                'total': sat_stats[0] or 0,
                'operational': sat_stats[1] or 0,
                'avg_altitude': float(sat_stats[2]) if sat_stats[2] else 0
            },
            'coverage': [
                {
                    'region': row[0],
                    'avg_coverage': float(row[1]) if row[1] else 0,
                    'avg_satellites': float(row[2]) if row[2] else 0
                }
                for row in coverage_data
            ],
            'performance': [
                {
                    'region': row[0],
                    'avg_latency': float(row[1]) if row[1] else 0,
                    'avg_throughput': float(row[2]) if row[2] else 0,
                    'avg_availability': float(row[3]) if row[3] else 0
                }
                for row in perf_data
            ]
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)

