from models import WeatherReading
from utils.logger import setup_logger

logger = setup_logger()

def clean_all_readings(reading: WeatherReading):
        """Validate and clean values before uploading to InfluxDB"""
        cleaned_readings = {}
        for field_name, value in reading.readings.items():
            value_clean = str(value).strip().replace(',', '.')
            try:
                # Cardinal directions are always strings
                if "cardinal" in field_name or "trend" in field_name:
                    cleaned_readings[field_name] = value_clean
                    continue

                numeric_value = float(value_clean)

                # Temperature fields (deg C)
                if field_name in [
                    "current_outside_temp", "current_dew_point", "current_wet_bulb"
                ]:
                    if -50 < numeric_value < 60:
                        cleaned_readings[field_name] = numeric_value
                elif field_name == "current_heat_index":
                    if -50 < numeric_value < 70:
                        cleaned_readings[field_name] = numeric_value
                elif field_name == "current_wind_chill":
                    if -80 < numeric_value < 60:
                        cleaned_readings[field_name] = numeric_value

                # Humidity (%)
                elif field_name == "current_outside_humidity":
                    if 0 <= numeric_value <= 100:
                        cleaned_readings[field_name] = numeric_value

                # Pressure (hPa)
                elif field_name in ["current_pressure", "current_sea_level_pressure"]:
                    if 800 <= numeric_value <= 1200:
                        cleaned_readings[field_name] = numeric_value
                # Wind speed (m/s)
                elif field_name in ["current_wind_speed", "average10_wind_speed"]:
                    if 0 <= numeric_value <= 150:
                        cleaned_readings[field_name] = numeric_value
                elif field_name == "gust_speed":
                    if 0 <= numeric_value <= 200:
                        cleaned_readings[field_name] = numeric_value

                # Wind direction (degrees)
                elif field_name in [
                    "average10_wind_direction", "max_wind_direction", "current_wind_direction"
                ]:
                    if 0 <= numeric_value < 360:
                        cleaned_readings[field_name] = numeric_value

                # Rain rate (mm/h)
                elif field_name == "current_rain_rate":
                    if 0 <= numeric_value <= 200:
                        cleaned_readings[field_name] = numeric_value

                # Rain total (mm)
                elif field_name == "total_rain":
                    if 0 <= numeric_value <= 500:
                        cleaned_readings[field_name] = numeric_value

                # Default: accept any float value
                else:
                    cleaned_readings[field_name] = numeric_value

            except ValueError:
                cleaned_readings[field_name] = value_clean  # Keep as string if not numeric

        reading.readings = cleaned_readings
        logger.info(f"Readings cleaned for station '{reading.station_tag}' at {reading.timestamp}: {cleaned_readings}")

        return reading