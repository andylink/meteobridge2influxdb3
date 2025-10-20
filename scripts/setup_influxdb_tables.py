# setup_influxdb_tables.py

import sys
from typing import List, Dict, Any

from config import settings
from utils import setup_logger
from models.weather import get_raw_fields_schema # IMPORTED function to get dynamic schema
import requests
import json # New import for logging the JSON payload

# Setup logging for the script
logger = setup_logger('influxdb_setup')

# --- FIELD DEFINITIONS FOR SCHEMA CREATION ---
# SENSOR_ROLLUP_FIELDS remains hardcoded as it represents aggregated data.
SENSOR_ROLLUP_FIELDS: List[Dict[str, str]] = [
    # --- Temperature Metrics ---
    {"name": "low_temperature", "type": "float64"},
    {"name": "time_of_low_temperature", "type": "utf8"}, # Time when the low occurred
    {"name": "high_temperature", "type": "float64"},
    {"name": "time_of_high_temperature", "type": "utf8"}, # Time when the high occurred
    {"name": "average_temperature", "type": "float64"},

    # --- Dew Point Metrics ---
    {"name": "low_dew_point", "type": "float64"},
    {"name": "time_of_low_dew_point", "type": "utf8"},
    {"name": "high_dew_point", "type": "float64"},
    {"name": "time_of_high_dew_point", "type": "utf8"},
    {"name": "average_dew_point", "type": "float64"},

    # --- Humidity Metrics ---
    {"name": "low_humidity", "type": "float64"},
    {"name": "time_of_low_humidity", "type": "utf8"},
    {"name": "high_humidity", "type": "float64"},
    {"name": "time_of_high_humidity", "type": "utf8"},
    {"name": "average_humidity", "type": "float64"},

    # --- Wind Metrics (Enhanced for Professional Service) ---
    {"name": "low_wind_speed", "type": "float64"},
    {"name": "time_of_low_wind_speed", "type": "utf8"},
    {"name": "high_avg_wind_speed", "type": "float64"}, # Max of the averaged wind speed readings
    {"name": "time_of_high_avg_wind_speed", "type": "utf8"},
    {"name": "average_wind_speed", "type": "float64"},

    {"name": "max_gust_speed", "type": "float64"},
    {"name": "time_of_max_gust", "type": "utf8"},        # Time of the maximum instantaneous gust
    {"name": "max_gust_direction", "type": "float64"},    # Direction corresponding to the max gust

    {"name": "wind_speed_stdev", "type": "float64"},      # Standard Deviation (measure of turbulence)
    {"name": "average_wind_direction", "type": "float64"},
    {"name": "dominant_wind_direction", "type": "utf8"},  # Cardinal direction (calculated by wind rose logic)
    {"name": "wind_run_total", "type": "float64"},        # Total air movement

    # --- Pressure Metrics ---
    {"name": "low_pressure", "type": "float64"},
    {"name": "time_of_low_pressure", "type": "utf8"},
    {"name": "high_pressure", "type": "float64"},
    {"name": "time_of_high_pressure", "type": "utf8"},
    {"name": "average_pressure", "type": "float64"},

    # --- Rainfall Metrics ---
    {"name": "max_rain_rate", "type": "float64"},
    {"name": "time_of_max_rain_rate", "type": "utf8"},
    {"name": "total_rainfall", "type": "float64"},
]


def setup_table_retention(host: str, token: str, database: str, table_name: str, retention_period: str, fields: List[Dict[str, str]]) -> bool:
    """
    Standalone function to ensure the table is created with a specific retention policy.
    This is called from a separate, one-time setup script.
    """
    api_url = f"{host}/api/v3/configure/table"
    
    payload = {
        "db": database,
        "table": table_name, 
        "tags": ["station"], # Tag columns are required
        "fields": fields,    
    }
    
    # Handle 'none' or 'INF' retention by omitting the key, resulting in infinite retention
    if retention_period.lower() not in ['none', 'inf']:
         payload['retention_period'] = retention_period
         retention_period_log = retention_period
    else:
        retention_period_log = "infinite"
    
    logger.info(f"Attempting to configure table '{table_name}' with {retention_period_log} retention...")
    
    # --- LOGGING: Log the full JSON payload being sent ---
    logger.info(f"API Payload for '{table_name}':\n{json.dumps(payload, indent=2)}")
    # -----------------------------------------------------
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=payload)
        
        if response.status_code in [200, 201]:
            logger.info(f"Successfully configured table '{table_name}' with {retention_period_log} retention.")
            return True
        
        elif response.status_code == 409:
             # Table already exists. 
             logger.info(f"Table '{table_name}' already exists. Skipping configuration attempt.")
             return True 
        
        response.raise_for_status() 

        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to configure table retention for '{table_name}': {e}")
        # Log the raw response content to help diagnose the 400 error
        if response is not None and response.content:
            logger.error(f"Response details: {response.text}")
        return False

def main():
    """Runs the one-time table configuration for InfluxDB."""
    logger.info("Starting InfluxDB table retention setup...")

    try:
        # Connection details pulled from config/settings
        host = settings.INFLUXDB_URL
        token = settings.INFLUXDB_TOKEN
        database = settings.INFLUXDB_DATABASE
        
        # --- GENERATE RAW FIELDS DYNAMICALLY ---
        RAW_FIELDS = get_raw_fields_schema()
        
        # --- LOGGING: Log the result of the dynamic schema generation ---
        logger.info(f"Dynamically generated RAW_FIELDS schema:\n{json.dumps(RAW_FIELDS, indent=2)}")
        # ----------------------------------------------------------------
        
        # Define all tables, their retention, and their expected fields
        table_config_map: Dict[str, Dict[str, Any]] = {
            "wind_sensor_raw": {'retention': '90d', 'fields': RAW_FIELDS}, 
            "wind_sensor_1h": {'retention': '1y', 'fields': SENSOR_ROLLUP_FIELDS}, 
            "wind_sensor_1d": {'retention': 'none', 'fields': SENSOR_ROLLUP_FIELDS}, 
        }
        
        success_count = 0
        total_count = len(table_config_map)

        # Setup all required tables
        for table_name, config in table_config_map.items():
            if setup_table_retention(host, token, database, table_name, config['retention'], config['fields']):
                success_count += 1
        
        if success_count == total_count:
            logger.info(f"Successfully configured all {total_count} tables. You do not need to run this script again.")
            sys.exit(0)
        else:
            logger.error(f"Failed to configure {total_count - success_count} out of {total_count} tables. Check logs for details.")
            sys.exit(1)

    except AttributeError as e:
        logger.error(f"Configuration error: Could not find connection setting: {e}. Ensure 'config.py' and your '.env' file are correctly loaded.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred during setup: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()