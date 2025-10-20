# influxdb.py

from influxdb_client_3 import InfluxDBClient3

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
       
        self.host = settings.INFLUXDB_URL
        self.token = settings.INFLUXDB_TOKEN
        self.database = settings.INFLUXDB_DATABASE
        self.raw_measurement_name = "wind_sensor_raw"
        
        # Webhook configuration (optional)
        self.webhook_url = os.getenv('WEBHOOK_URL')
        self.webhook_token = os.getenv('WEBHOOK_TOKEN')
        
        # NOTE: All table configuration logic has been REMOVED from __init__ 
        # and moved to setup_influxdb_tables.py.
        
    def _send_to_webhook(self, reading: WeatherReading) -> bool:
        """Send weather reading to webhook endpoint"""
        # ... (method body remains unchanged) ...
        if not self.webhook_url or not self.webhook_token:
            logger.debug("Webhook not configured, skipping webhook POST")
            return True
        
        try:
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
        # ... (method body remains unchanged) ...
        try:
            # --- 1. Prepare data for DataFrame creation ---
            data = {}
            data['station'] = reading.station_tag 

            field_data = {}
            for field_name, value_str in reading.readings.items():
                value_clean = value_str.strip().replace(',', '.')
                
                if value_clean in ("", "-"):
                    logger.debug(f"Skipping empty value for {field_name}")
                    continue
                
                try:
                    numeric_value = float(value_clean)
                    field_data[field_name] = numeric_value
                    logger.debug(f"Added numeric field: {field_name}={numeric_value}")
                except ValueError:
                    field_data[field_name] = value_clean
                    logger.debug(f"Added string field: {field_name}='{value_clean}'")

            data.update(field_data)
            
            # --- 2. Create Pandas DataFrame ---
            data_list = [data]
            timestamps = [reading.timestamp] 
            
            df = pd.DataFrame(data_list)
            df.index = pd.to_datetime(timestamps, unit='ns') 
            
            # --- 3. Write to InfluxDB using v3 Client ---
            with InfluxDBClient3(host=self.host, token=self.token, database=self.database) as client:
                client.write(
                    df, 
                    data_frame_measurement_name=self.raw_measurement_name, 
                    data_frame_tag_columns=['station']
                )
                
            logger.info(f"Successfully wrote data point to database '{self.database}'")
            
            self._send_to_webhook(reading)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to write to InfluxDB: {e}")
            return False