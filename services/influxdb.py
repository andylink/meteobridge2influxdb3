from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from config import settings
from models import WeatherReading
from utils import setup_logger


logger = setup_logger('influxdb')


class InfluxDBService:
    """Service for writing weather data to InfluxDB"""
    
    def __init__(self):
        self.url = settings.INFLUXDB_URL
        self.token = settings.INFLUXDB_TOKEN
        self.org = settings.INFLUXDB_ORG
        self.bucket = settings.INFLUXDB_BUCKET
        
    def write_reading(self, reading: WeatherReading) -> bool:
        """Write a weather reading to InfluxDB"""
        try:
            point = Point("weather").tag("station", reading.station_tag).time(
                reading.timestamp, WritePrecision.NS
            )
            
            # Add fields to point
            for field_name, value_str in reading.readings.items():
                value_clean = value_str.strip().replace(',', '.')
                
                # Skip empty or invalid values
                if value_clean in ("", "-"):
                    logger.debug(f"Skipping empty value for {field_name}")
                    continue
                
                try:
                    # Try to convert to numeric
                    numeric_value = float(value_clean)
                    point.field(field_name, numeric_value)
                    logger.debug(f"Added numeric field: {field_name}={numeric_value}")
                except ValueError:
                    # Store as string if not numeric
                    point.field(field_name, value_clean)
                    logger.debug(f"Added string field: {field_name}='{value_clean}'")
            
            # Write to InfluxDB
            with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
                write_api.write(bucket=self.bucket, org=self.org, record=point)
                
            logger.info(f"Successfully wrote data point to bucket '{self.bucket}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write to InfluxDB: {e}")
            return False