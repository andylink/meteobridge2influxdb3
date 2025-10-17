import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Settings:
    """Application settings loaded from environment variables"""
    
    # Meteobridge Configuration
    METEOBRIDGE_IP: str = os.getenv('METEOBRIDGE_IP')
    METEOBRIDGE_USERNAME: str = os.getenv('METEOBRIDGE_USERNAME')
    METEOBRIDGE_PASSWORD: str = os.getenv('METEOBRIDGE_PASSWORD')
    METEOBRIDGE_STATION_TAG: str = os.getenv('METEOBRIDGE_STATION_TAG')
    METEOBRIDGE_TIMEZONE: str = os.getenv('METEOBRIDGE_TIMEZONE')
    
    # InfluxDB Configuration
    INFLUXDB_URL: str = os.getenv('INFLUXDB_URL')
    INFLUXDB_TOKEN: str = os.getenv('INFLUXDB_TOKEN')
    INFLUXDB_ORG: str = os.getenv('INFLUXDB_ORG')
    INFLUXDB_BUCKET: str = os.getenv('INFLUXDB_BUCKET')
    
    # Application Configuration
    COLLECTION_INTERVAL: int = int(os.getenv('COLLECTION_INTERVAL', '10'))  # Kept for backward compatibility
    
    # Specific collection intervals for different sensor types
    TEMP_HUMIDITY_INTERVAL: int = int(os.getenv('TEMP_HUMIDITY_INTERVAL', '60'))
    WIND_INTERVAL: int = int(os.getenv('WIND_INTERVAL', '10'))
    RAIN_INTERVAL: int = int(os.getenv('RAIN_INTERVAL', '60'))
    PRESSURE_INTERVAL: int = int(os.getenv('PRESSURE_INTERVAL', '60'))
    
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    def __repr__(self) -> str:
        return f"Settings(station={self.METEOBRIDGE_STATION_TAG}, interval={self.COLLECTION_INTERVAL}s)"


settings = Settings()