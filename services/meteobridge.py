import requests
from urllib.parse import unquote
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Tuple, Dict

from config import settings
from models import SensorDefinition, WeatherReading, SENSOR_DEFINITIONS
from utils import setup_logger


logger = setup_logger('meteobridge')


class MeteobridgeClient:
    """Client for interacting with Meteobridge weather station"""
    
    def __init__(self, sensors: List[SensorDefinition] = None):
        self.base_ip = settings.METEOBRIDGE_IP
        self.username = settings.METEOBRIDGE_USERNAME
        self.password = settings.METEOBRIDGE_PASSWORD
        self.station_tag = settings.METEOBRIDGE_STATION_TAG
        self.timezone = ZoneInfo(settings.METEOBRIDGE_TIMEZONE)
        self.sensors = sensors or SENSOR_DEFINITIONS
        
    def _build_template(self) -> str:
        """Build Meteobridge template string"""
        header = "[DD].[MM].[YY] [HH]:[mm]:[ss][APM]"
        sensor_parts = [
            f"[{s.sensor_id}-{s.selector}]" if s.selector else f"[{s.sensor_id}]"
            for s in self.sensors
        ]
        template = header + "\r" + "\r".join(sensor_parts)
        return template
    
    def fetch_data(self) -> WeatherReading:
        """Fetch current weather data from Meteobridge"""
        template = self._build_template()
        params = {
            "template": template,
            "contenttype": "text/plain;charset=iso-8859-1"
        }
        url = f"http://{self.base_ip}/cgi-bin/template.cgi"
        
        try:
            logger.debug(f"Fetching data from {url}")
            response = requests.get(
                url,
                params=params,
                auth=(self.username, self.password),
                timeout=6
            )
            response.raise_for_status()
            
            decoded = unquote(response.text)
            logger.debug(f"Raw response: {repr(decoded)}")
            
            lines = decoded.replace("\r", "\n").splitlines()
            
            if not lines:
                raise RuntimeError("Empty response from Meteobridge")
            
            # Parse timestamp
            timestamp_str = lines[0].strip()
            dt_local = datetime.strptime(timestamp_str, "%d.%m.%y %I:%M:%S%p")
            dt_aware = dt_local.replace(tzinfo=self.timezone)
            dt_utc = dt_aware.astimezone(ZoneInfo("UTC"))
            
            # Parse readings
            readings = {
                sensor.field_name: line.strip()
                for sensor, line in zip(self.sensors, lines[1:])
            }
            logger.info(f"Parsed readings: {readings}")
            weather_reading = WeatherReading(
                timestamp=dt_utc,
                readings=readings,
                station_tag=self.station_tag
            )
            
            logger.info(f"Successfully fetched data: {len(readings)} readings at {dt_utc}")
            return weather_reading
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from Meteobridge: {e}")
            raise
        except ValueError as e:
            logger.error(f"Failed to parse timestamp: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching Meteobridge data: {e}")
            raise