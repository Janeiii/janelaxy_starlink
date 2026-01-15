"""
Satellite Simulator for Starlink Data Generation
Generates realistic satellite positions, orbital parameters, and telemetry data
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import math


class SatelliteSimulator:
    """Simulates Starlink satellite constellation behavior"""
    
    def __init__(self, num_satellites: int = 100, orbit_altitude_km: float = 550.0):
        """
        Initialize satellite simulator
        
        Args:
            num_satellites: Number of satellites to simulate
            orbit_altitude_km: Orbital altitude in kilometers
        """
        self.num_satellites = num_satellites
        self.orbit_altitude_km = orbit_altitude_km
        self.earth_radius_km = 6371.0
        self.orbit_radius_km = self.earth_radius_km + orbit_altitude_km
        
        # Initialize satellite IDs
        self.satellite_ids = [f"STARLINK-{i:04d}" for i in range(1, num_satellites + 1)]
        
    def calculate_orbital_velocity(self) -> float:
        """Calculate orbital velocity in km/s"""
        G = 6.67430e-11  # Gravitational constant
        M_earth = 5.972e24  # Earth mass in kg
        r_m = self.orbit_radius_km * 1000  # Convert to meters
        
        velocity_ms = math.sqrt(G * M_earth / r_m)
        return velocity_ms / 1000  # Convert to km/s
    
    def generate_orbital_parameters(self, satellite_id: str) -> Dict:
        """Generate orbital parameters for a satellite"""
        np.random.seed(hash(satellite_id) % 2**32)
        
        # Inclination (Starlink uses ~53 degrees)
        inclination = 53.0 + np.random.uniform(-2, 2)
        
        # Right ascension of ascending node (RAAN)
        raan = np.random.uniform(0, 360)
        
        # Argument of perigee
        argument_of_perigee = np.random.uniform(0, 360)
        
        # Mean anomaly (initial position in orbit)
        mean_anomaly = np.random.uniform(0, 360)
        
        return {
            'inclination': inclination,
            'raan': raan,
            'argument_of_perigee': argument_of_perigee,
            'mean_anomaly': mean_anomaly
        }
    
    def calculate_position(self, satellite_id: str, timestamp: datetime, 
                          orbital_params: Dict) -> Tuple[float, float, float]:
        """
        Calculate satellite position at given timestamp
        
        Returns:
            (latitude, longitude, altitude)
        """
        # Simplified orbital mechanics
        # Orbital period in minutes (for 550km altitude ~95 minutes)
        orbital_period_minutes = 95.0
        
        # Time since epoch (use satellite ID hash as epoch offset)
        epoch_offset = hash(satellite_id) % 1000
        time_elapsed = (timestamp.hour * 60 + timestamp.minute + epoch_offset) % orbital_period_minutes
        
        # Mean anomaly progression
        mean_anomaly_deg = (orbital_params['mean_anomaly'] + 
                           (time_elapsed / orbital_period_minutes) * 360) % 360
        
        # Simplified: assume circular orbit
        # Convert mean anomaly to true anomaly (simplified)
        true_anomaly = math.radians(mean_anomaly_deg)
        
        # Calculate position in orbital plane
        # For simplicity, use spherical coordinates
        inclination_rad = math.radians(orbital_params['inclination'])
        raan_rad = math.radians(orbital_params['raan'])
        
        # Simplified position calculation
        # This is a simplified model - real calculations would use proper orbital mechanics
        latitude = math.degrees(math.asin(math.sin(inclination_rad) * math.sin(true_anomaly)))
        longitude = (math.degrees(raan_rad) + 
                    math.degrees(math.atan2(math.cos(inclination_rad) * math.sin(true_anomaly),
                                           math.cos(true_anomaly)))) % 360 - 180
        
        altitude = self.orbit_altitude_km + np.random.uniform(-5, 5)
        
        return latitude, longitude, altitude
    
    def generate_satellite_data(self, timestamp: datetime) -> pd.DataFrame:
        """Generate satellite position data for all satellites at a given timestamp"""
        data = []
        velocity = self.calculate_orbital_velocity()
        
        for sat_id in self.satellite_ids:
            orbital_params = self.generate_orbital_parameters(sat_id)
            lat, lon, alt = self.calculate_position(sat_id, timestamp, orbital_params)
            
            # Generate status (operational, maintenance, etc.)
            status_prob = np.random.random()
            if status_prob > 0.95:
                status = "maintenance"
            elif status_prob > 0.90:
                status = "standby"
            else:
                status = "operational"
            
            data.append({
                'satellite_id': sat_id,
                'timestamp': timestamp,
                'latitude': lat,
                'longitude': lon,
                'altitude': alt,
                'velocity': velocity + np.random.uniform(-0.1, 0.1),
                'status': status
            })
        
        return pd.DataFrame(data)
    
    def generate_coverage_data(self, timestamp: datetime, 
                              regions: List[Dict] = None) -> pd.DataFrame:
        """Generate coverage data for different regions"""
        if regions is None:
            # Default regions (major areas)
            regions = [
                {'region_id': 'NA', 'name': 'North America', 'lat': 45.0, 'lon': -100.0},
                {'region_id': 'EU', 'name': 'Europe', 'lat': 50.0, 'lon': 10.0},
                {'region_id': 'AS', 'name': 'Asia', 'lat': 35.0, 'lon': 100.0},
                {'region_id': 'SA', 'name': 'South America', 'lat': -15.0, 'lon': -60.0},
                {'region_id': 'OC', 'name': 'Oceania', 'lat': -25.0, 'lon': 135.0},
            ]
        
        coverage_data = []
        satellite_df = self.generate_satellite_data(timestamp)
        
        for region in regions:
            # Calculate satellites in range (simplified: within 2000km)
            region_lat, region_lon = region['lat'], region['lon']
            
            # Count satellites in range
            distances = []
            for _, sat in satellite_df.iterrows():
                # Haversine distance (simplified)
                lat_diff = abs(sat['latitude'] - region_lat)
                lon_diff = abs(sat['longitude'] - region_lon)
                distance = math.sqrt(lat_diff**2 + lon_diff**2) * 111  # Approximate km
                distances.append(distance)
            
            in_range = sum(1 for d in distances if d < 2000)
            satellite_count = in_range
            
            # Coverage percentage (simplified model)
            coverage_percentage = min(100, (satellite_count / 10) * 100)
            
            # Average signal strength (dBm)
            avg_signal_strength = -80 + (coverage_percentage / 100) * 20 + np.random.uniform(-5, 5)
            
            coverage_data.append({
                'region_id': region['region_id'],
                'region_name': region['name'],
                'timestamp': timestamp,
                'coverage_percentage': round(coverage_percentage, 2),
                'satellite_count': satellite_count,
                'avg_signal_strength': round(avg_signal_strength, 2)
            })
        
        return pd.DataFrame(coverage_data)
    
    def generate_performance_metrics(self, timestamp: datetime, 
                                    regions: List[str] = None) -> pd.DataFrame:
        """Generate network performance metrics"""
        if regions is None:
            regions = ['NA', 'EU', 'AS', 'SA', 'OC']
        
        metrics = []
        
        for region in regions:
            # Simulate realistic performance metrics
            base_latency = np.random.uniform(20, 50)  # ms
            base_throughput = np.random.uniform(50, 200)  # Mbps
            
            # Add some variation based on time of day
            hour_factor = 1.0 + 0.2 * math.sin(timestamp.hour * math.pi / 12)
            
            latency_ms = base_latency * hour_factor
            throughput_mbps = base_throughput / hour_factor
            
            # Availability (typically 99%+)
            availability_percentage = 99.0 + np.random.uniform(-1, 1)
            
            metrics.append({
                'timestamp': timestamp,
                'region': region,
                'latency_ms': round(latency_ms, 2),
                'throughput_mbps': round(throughput_mbps, 2),
                'availability_percentage': round(availability_percentage, 2)
            })
        
        return pd.DataFrame(metrics)

