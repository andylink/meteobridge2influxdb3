from influxdb_client_3 import InfluxDBClient3
# InfluxDB v3 client does not use Point, WritePrecision, or SYNCHRONOUS directly
# from the v2 client, so we remove those imports.

from config import settings
from models import WeatherReading
from utils import setup_logger

import pandas as pd # Use pandas for data structuring

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
            return True
            
        except Exception as e:
            # It's better to catch the specific exception from the v3 client 
            # (e.g., InfluxDBError) if possible, but a generic Exception works for now.
            logger.error(f"Failed to write to InfluxDB: {e}")
            return False