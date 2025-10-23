# rollup_service.py
# -----------------
# This module defines a WeatherRollupService class responsible for:
# - Fetching raw weather data from InfluxDB (at fine-grained intervals)
# - Aggregating the data into hourly and daily "rollups" (summaries)
# - Writing computed metrics (averages, highs/lows, totals) back to InfluxDB

from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from influxdb_client_3 import InfluxDBClient3

from config import settings
from utils import setup_logger

logger = setup_logger('rollup_service')


class WeatherRollupService:
    """Service for computing and writing weather data rollups (hourly and daily)."""
    
    def __init__(self):
        # Initialize InfluxDB connection settings from config
        self.host = settings.INFLUXDB_URL
        self.token = settings.INFLUXDB_TOKEN
        self.database = settings.INFLUXDB_DATABASE
        self.raw_table = settings.INFLUXDB_RAW_TABLE
        self.hourly_table = settings.INFLUXDB_HOURLY_TABLE
        self.daily_table = settings.INFLUXDB_DAILY_TABLE

    # -------------------------------------------------------------------------
    # Utility Functions
    # -------------------------------------------------------------------------
    
    def _get_cardinal_direction(self, degrees: float) -> str:
        """Convert a numeric wind direction (0–360°) into a human-readable cardinal direction."""
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                     'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        # Each direction represents 22.5° (360° / 16)
        index = round(degrees / 22.5) % 16
        return directions[index]
    
    def _calculate_wind_run(self, wind_speeds: pd.Series, interval_minutes: int) -> float:
        """
        Calculate the total 'wind run' — the distance air has traveled over time.
        Formula: wind_run = mean_speed * total_hours.
        The interval_minutes parameter defines the spacing between each reading.
        """
        hours = interval_minutes / 60.0
        return (wind_speeds.mean() * hours * len(wind_speeds))
    
    def _find_value_with_time(self, series: pd.Series, agg_func) -> tuple:
        """
        Find the min or max value in a series along with its corresponding timestamp.
        Returns a tuple: (value, timestamp_str)
        """
        if series.empty or series.isna().all():
            return (None, None)
        
        # Compute value (min or max)
        value = agg_func(series)
        # Find the first occurrence of that value in the series
        filtered = series[series == value]
        if filtered.empty:
            return (float(value), None)
        timestamp = filtered.index[0]
        return (float(value), timestamp.isoformat())
    
    # -------------------------------------------------------------------------
    # Core Rollup Computation
    # -------------------------------------------------------------------------
    
    def _compute_rollup_metrics(self, df: pd.DataFrame, station: str) -> Dict:
        """
        Compute all rollup statistics (temperature, wind, pressure, etc.)
        from the raw DataFrame of weather station data.
        Returns a dictionary of aggregated metrics.
        """
        metrics = {'station': station}
        
        # --- Temperature Metrics ---
        if 'current_outside_temp' in df.columns:
            temp = df['current_outside_temp'].dropna()
            if not temp.empty:
                metrics['low_temperature'], metrics['time_of_low_temperature'] = \
                    self._find_value_with_time(temp, np.min)
                metrics['high_temperature'], metrics['time_of_high_temperature'] = \
                    self._find_value_with_time(temp, np.max)
                metrics['average_temperature'] = float(temp.mean())
        
        # --- Dew Point Metrics ---
        if 'current_dew_point' in df.columns:
            dew = df['current_dew_point'].dropna()
            if not dew.empty:
                metrics['low_dew_point'], metrics['time_of_low_dew_point'] = \
                    self._find_value_with_time(dew, np.min)
                metrics['high_dew_point'], metrics['time_of_high_dew_point'] = \
                    self._find_value_with_time(dew, np.max)
                metrics['average_dew_point'] = float(dew.mean())
        
        # --- Humidity Metrics ---
        if 'current_outside_humidity' in df.columns:
            hum = df['current_outside_humidity'].dropna()
            if not hum.empty:
                metrics['low_humidity'], metrics['time_of_low_humidity'] = \
                    self._find_value_with_time(hum, np.min)
                metrics['high_humidity'], metrics['time_of_high_humidity'] = \
                    self._find_value_with_time(hum, np.max)
                metrics['average_humidity'] = float(hum.mean())
        
        # --- Wind Speed Metrics ---
        if 'current_wind_speed' in df.columns:
            wind = df['current_wind_speed'].dropna()
            if not wind.empty:
                metrics['low_wind_speed'], metrics['time_of_low_wind_speed'] = \
                    self._find_value_with_time(wind, np.min)
                metrics['high_avg_wind_speed'], metrics['time_of_high_avg_wind_speed'] = \
                    self._find_value_with_time(wind, np.max)
                metrics['average_wind_speed'] = float(wind.mean())
                metrics['wind_speed_stdev'] = float(wind.std())
                
                # Wind run = total distance traveled by the air over the period
                metrics['wind_run_total'] = self._calculate_wind_run(wind, settings.WIND_INTERVAL / 60)
        
        # --- Gust Metrics ---
        if 'gust_speed' in df.columns:
            gust = df['gust_speed'].dropna()
            if not gust.empty:
                metrics['max_gust_speed'], metrics['time_of_max_gust'] = \
                    self._find_value_with_time(gust, np.max)
                
                # Find the wind direction at the time of the strongest gust
                if metrics['time_of_max_gust'] and 'max_wind_direction' in df.columns:
                    gust_time = pd.to_datetime(metrics['time_of_max_gust'])
                    gust_dir = df.loc[gust_time, 'max_wind_direction']
                    if pd.notna(gust_dir):
                        metrics['max_gust_direction'] = float(gust_dir)
        
        # --- Wind Direction Metrics ---
        if 'current_wind_direction' in df.columns:
            wind_dir = df['current_wind_direction'].dropna()
            if not wind_dir.empty:
                # Compute average (circular) wind direction
                angles_rad = np.radians(wind_dir)
                sin_avg = np.sin(angles_rad).mean()
                cos_avg = np.cos(angles_rad).mean()
                avg_angle = np.degrees(np.arctan2(sin_avg, cos_avg))
                if avg_angle < 0:
                    avg_angle += 360
                metrics['average_wind_direction'] = float(avg_angle)
                
                # Determine dominant wind direction (most frequent sector)
                sectors = (wind_dir / 22.5).round() * 22.5
                mode = sectors.mode()
                if not mode.empty:
                    dominant_deg = float(mode[0])
                    metrics['dominant_wind_direction'] = self._get_cardinal_direction(dominant_deg)
        
        # --- Pressure Metrics ---
        if 'current_sea_level_pressure' in df.columns:
            press = df['current_sea_level_pressure'].dropna()
            if not press.empty:
                metrics['low_pressure'], metrics['time_of_low_pressure'] = \
                    self._find_value_with_time(press, np.min)
                metrics['high_pressure'], metrics['time_of_high_pressure'] = \
                    self._find_value_with_time(press, np.max)
                metrics['average_pressure'] = float(press.mean())
        
        # --- Rainfall Metrics ---
        if 'current_rain_rate' in df.columns:
            rain_rate = df['current_rain_rate'].dropna()
            if not rain_rate.empty:
                metrics['max_rain_rate'], metrics['time_of_max_rain_rate'] = \
                    self._find_value_with_time(rain_rate, np.max)
        
        if 'total_rain' in df.columns:
            rain = df['total_rain'].dropna()
            if not rain.empty:
                # Total rainfall = difference between last and first cumulative readings
                metrics['total_rainfall'] = float(rain.iloc[-1] - rain.iloc[0])
        
        return metrics
    
    # -------------------------------------------------------------------------
    # Database Query + Write Operations
    # -------------------------------------------------------------------------
    
    def _query_raw_data(self, start_time: datetime, end_time: datetime, 
                        station: str = settings.METEOBRIDGE_STATION_TAG) -> Optional[pd.DataFrame]:
        """
        Query raw (high-frequency) weather data from InfluxDB for a given time window.
        Returns a pandas DataFrame indexed by timestamp, or None if no data.
        """
        try:
            query = f"""
                SELECT *
                FROM "{self.raw_table}"
                WHERE station = '{station}'
                  AND time >= '{start_time.isoformat()}Z'
                  AND time < '{end_time.isoformat()}Z'
                ORDER BY time
            """
            
            # Connect to InfluxDB and execute query
            with InfluxDBClient3(host=self.host, token=self.token, database=self.database) as client:
                result = client.query(query=query, language="sql")
                
            if result is None:
                logger.warning(f"No data found for {station} between {start_time} and {end_time}")
                return None

            # Convert results into a DataFrame
            df = result.to_pandas()
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df = df.set_index('time')

            if df.empty:
                logger.warning(f"No data found for {station} between {start_time} and {end_time}")
                return None

            logger.info(f"Retrieved {len(result)} raw records for {station}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to query raw data: {e}")
            return None
    
    def _write_rollup(self, metrics: Dict, timestamp: datetime, table_name: str) -> bool:
        """
        Write a dictionary of computed metrics into the specified InfluxDB rollup table.
        The timestamp marks the period the rollup represents (hourly or daily).
        """
        try:
            df = pd.DataFrame([metrics])
            df.index = pd.DatetimeIndex([timestamp])
            
            with InfluxDBClient3(host=self.host, token=self.token, database=self.database) as client:
                client.write(
                    df,
                    data_frame_measurement_name=table_name,
                    data_frame_tag_columns=['station']
                )
            
            logger.info(f"Successfully wrote rollup to {table_name} for {timestamp}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write rollup to {table_name}: {e}")
            return False
    
    # -------------------------------------------------------------------------
    # Public Rollup Operations
    # -------------------------------------------------------------------------
    
    def compute_hourly_rollup(self, target_hour: Optional[datetime] = None, 
                             station: str = settings.METEOBRIDGE_STATION_TAG) -> bool:
        """
        Compute and write an hourly rollup for the given hour.
        If no hour is specified, compute the *previous* complete hour.
        """
        if target_hour is None:
            now = datetime.utcnow()
            target_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        
        start_time = target_hour
        end_time = target_hour + timedelta(hours=1)
        
        logger.info(f"Computing hourly rollup for {start_time} to {end_time}")
        
        # Fetch raw data, compute metrics, and write to InfluxDB
        df = self._query_raw_data(start_time, end_time, station)
        if df is None:
            return False
        
        metrics = self._compute_rollup_metrics(df, station)
        return self._write_rollup(metrics, target_hour, self.hourly_table)
    
    def compute_daily_rollup(self, target_date: Optional[datetime] = None, 
                            station: str = settings.METEOBRIDGE_STATION_TAG) -> bool:
        """
        Compute and write a daily rollup for the given date.
        If no date is specified, defaults to 'yesterday' (UTC).
        """
        if target_date is None:
            now = datetime.utcnow()
            target_date = (now - timedelta(days=1)).replace(hour=0, minute=0, 
                                                            second=0, microsecond=0)
        
        start_time = target_date
        end_time = target_date + timedelta(days=1)
        
        logger.info(f"Computing daily rollup for {start_time} to {end_time}")
        
        # Fetch raw data, compute metrics, and write to InfluxDB
        df = self._query_raw_data(start_time, end_time, station)
        if df is None:
            return False
        
        metrics = self._compute_rollup_metrics(df, station)
        return self._write_rollup(metrics, target_date, self.daily_table)
