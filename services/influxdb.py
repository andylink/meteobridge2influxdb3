from influxdb_client_3 import InfluxDBClient3
# InfluxDB v3 client does not use Point, WritePrecision, or SYNCHRONOUS directly
# from the v2 client, so we remove those imports.

from config import settings
from models import WeatherReading
from utils import setup_logger

import os
import pandas as pd # Use pandas for data structuring
import requests
from datetime import datetime

logger = setup_logger('influxdb')


class InfluxDBService:
    """Service for writing weather data to InfluxDB"""
    
    def __init__(self):
        # The v3 client combines these into a single DSN (like URL) and uses
        # database name instead of bucket/org for the write operation.
        self.host = settings.INFLUXDB_URL
        self.token = settings.INFLUXDB_TOKEN
        self.database = settings.INFLUXDB_BUCKET # Use bucket as the database name
        self.measurement_name = "weather"
        
        # Webhook configuration (optional)
        self.webhook_url = os.getenv('WEBHOOK_URL')  # e.g., http://localhost:5000/webhook
        self.webhook_token = os.getenv('WEBHOOK_TOKEN')  # Token for X-WEBHOOK-TOKEN header
    
    def _send_to_webhook(self, reading: WeatherReading) -> bool:
        """Send weather reading to webhook endpoint"""
        if not self.webhook_url or not self.webhook_token:
            logger.debug("Webhook not configured, skipping webhook POST")
            return True
        
        try:
            # Prepare payload matching the webhook API format
            payload = {
                "received_at": datetime.utcnow().isoformat() + "Z",
                "payload": {
                    "station": reading.station_tag,
                    "timestamp": reading.timestamp.isoformat(),
                    "readings": reading.readings
                }
            }
            
            headers = {
                "Content-Type": "application/json",
                "X-WEBHOOK-TOKEN": self.webhook_token
            }
            
            response = requests.post(
                self.webhook_url, 
                json=payload, 
                headers=headers, 
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully sent data to webhook at {self.webhook_url}")
                return True
            else:
                logger.warning(f"Webhook POST failed: {response.status_code} {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logger.error(f"Webhook request timed out: {self.webhook_url}")
            return False
        except Exception as e:
            logger.error(f"Failed to send to webhook: {e}")
            return False
        
    def write_reading(self, reading: WeatherReading) -> bool:
        """Write a weather reading to InfluxDB"""
        try:
            # --- 1. Prepare data for DataFrame creation ---
            data = {}
            # Tags become columns with object/string dtype
            data['station'] = reading.station_tag 
            # Timestamp (index) must be in appropriate datetime format (e.g., nanoseconds)
            # The timestamp is typically set as the DataFrame index.

            # Fields (values)
            field_data = {}
            for field_name, value_str in reading.readings.items():
                value_clean = value_str.strip().replace(',', '.')
                
                # Skip empty or invalid values
                if value_clean in ("", "-"):
                    logger.debug(f"Skipping empty value for {field_name}")
                    continue
                
                try:
                    # Try to convert to numeric (float)
                    numeric_value = float(value_clean)
                    field_data[field_name] = numeric_value
                    logger.debug(f"Added numeric field: {field_name}={numeric_value}")
                except ValueError:
                    # Store as string if not numeric
                    field_data[field_name] = value_clean
                    logger.debug(f"Added string field: {field_name}='{value_clean}'")

            # Combine tags and fields into a single row structure (dictionary)
            # Since we are writing one reading at a time, this is a dictionary
            # representing one row of the dataframe.
            data.update(field_data)
            
            # --- 2. Create Pandas DataFrame ---
            # Create a list of the data dictionary for DataFrame construction
            data_list = [data]
            
            # Create the index (timestamp column)
            timestamps = [reading.timestamp] 
            
            df = pd.DataFrame(data_list)
            df.index = pd.to_datetime(timestamps, unit='ns') # Set timestamp as index, matching original WritePrecision.NS
            
            # --- 3. Write to InfluxDB using v3 Client ---
            # NOTE: The v3 client connects to a database *on initialization*.
            # The `org` is often embedded in the host/token setup or less emphasized
            # when using InfluxDB Cloud/OSS with Flight SQL enabled.
            # We use host, token, and database (which maps to the bucket).

            # Using a context manager for the client is good practice.
            with InfluxDBClient3(host=self.host, token=self.token, database=self.database) as client:
                # The write method for v3 client accepts the DataFrame,
                # the measurement name (formerly "weather"), and tags/field handling.
                client.write(
                    df, 
                    data_frame_measurement_name=self.measurement_name, 
                    data_frame_tag_columns=['station']
                )
                
            logger.info(f"Successfully wrote data point to database/bucket '{self.database}'")
            
            # Send to webhook after successful InfluxDB write
            self._send_to_webhook(reading)
            
            return True
            
        except Exception as e:
            # It's better to catch the specific exception from the v3 client 
            # (e.g., InfluxDBError) if possible, but a generic Exception works for now.
            logger.error(f"Failed to write to InfluxDB: {e}")
            return False