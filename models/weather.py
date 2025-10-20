from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List


@dataclass
class SensorDefinition:
    """Definition of a Meteobridge sensor"""
    sensor_id: str
    selector: str
    field_name: str
    sensor_type: str = "other"  # Type for grouping: temp_humidity, wind, rain, pressure
    
    def __post_init__(self):
        """Validate sensor definition"""
        if not self.sensor_id or not self.field_name:
            raise ValueError("sensor_id and field_name are required")


@dataclass
class WeatherReading:
    """Weather data reading with timestamp"""
    timestamp: datetime
    readings: Dict[str, str]
    station_tag: str
    
    def __repr__(self) -> str:
        return f"WeatherReading(timestamp={self.timestamp}, fields={len(self.readings)})"


def get_sensors_by_type(sensor_type: str) -> List[SensorDefinition]:
    """Get all sensors of a specific type"""
    return [sensor for sensor in SENSOR_DEFINITIONS if sensor.sensor_type == sensor_type]


def get_sensor_types() -> List[str]:
    """Get all unique sensor types"""
    return list(set(sensor.sensor_type for sensor in SENSOR_DEFINITIONS))

def get_raw_fields_schema() -> List[Dict[str, str]]:
    """
    Generates the InfluxDB field schema (name and type) from SENSOR_DEFINITIONS.
    Infers type based on field name: 'cardinal' fields are varchar, all others are float64.
    """
    schema = []
    
    # Fields containing these substrings are treated as strings (varchar)
    STRING_FIELD_SUBSTRINGS = ["cardinal"]
    
    for sensor in SENSOR_DEFINITIONS:
        field_name = sensor.field_name
        
        # Default to float64 (numeric)
        data_type = "float64"
        
        # Check if the field name should be a string
        for substring in STRING_FIELD_SUBSTRINGS:
            if substring in field_name:
                data_type = "utf8"
                break
        
        schema.append({"name": field_name, "type": data_type})
        
    return schema 

# Sensor definitions
SENSOR_DEFINITIONS: List[SensorDefinition] = [
    SensorDefinition("th0temp", "act", "current_outside_temp", "temp_humidity"),
    SensorDefinition("th0hum", "act", "current_outside_humidity", "temp_humidity"),
    SensorDefinition("th0dew", "act", "current_dew_point", "temp_humidity"),
    SensorDefinition("th0heatindex", "act", "current_heat_index", "temp_humidity"),
    SensorDefinition("th0wetbulb", "act", "current_wet_bulb", "temp_humidity"),
    SensorDefinition("thb0press", "act", "current_pressure", "pressure"),
    SensorDefinition("thb0seapress", "act", "current_sea_level_pressure", "pressure"),
    SensorDefinition("wind0wind", "act", "current_wind_speed", "wind"),
    SensorDefinition("wind0wind", "avg10", "average10_wind_speed", "wind"),
    SensorDefinition("wind0dir", "avg10=endir", "average10_wind_cardinal", "wind"),
    SensorDefinition("wind0dir", "avg10", "average10_wind_direction", "wind"),
    SensorDefinition("wind0dir", "act=endir", "current_wind_cardinal", "wind"),
    SensorDefinition("wind0dir", "act", "current_wind_direction", "wind"),
    SensorDefinition("wind0maxdir", "act", "max_wind_direction", "wind"),
    SensorDefinition("wind0wind", "max10", "gust_speed", "wind"),
    SensorDefinition("wind0chill", "act", "current_wind_chill", "wind"),
    SensorDefinition("rain0rate", "act", "current_rain_rate", "rain"),
    SensorDefinition("rain0total", "sumday", "total_rain", "rain"),
]
