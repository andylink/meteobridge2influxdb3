"""
Weather Station Data Collector
Collects data from Meteobridge and stores in InfluxDB at configurable intervals
"""

import signal
import sys
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import settings
from services import MeteobridgeClient, InfluxDBService
from models import get_sensors_by_type, get_sensor_types
from utils import setup_logger


logger = setup_logger('main')
scheduler = BlockingScheduler()


def collect_and_store_data(sensor_type: str = None):
    """Main data collection function for specific sensor type or all sensors"""
    try:
        type_desc = f" ({sensor_type})" if sensor_type else ""
        logger.info(f"Starting data collection cycle{type_desc}...")
        
        # Initialize services
        sensors = get_sensors_by_type(sensor_type) if sensor_type else None
        meteobridge = MeteobridgeClient(sensors=sensors)
        influxdb = InfluxDBService()
        
        # Fetch data from Meteobridge
        reading = meteobridge.fetch_data()
        logger.info(f"Fetched {len(reading.readings)} sensor readings{type_desc}")
        
        # Write to InfluxDB
        success = influxdb.write_reading(reading)
        
        if success:
            logger.info(f"Data collection cycle{type_desc} completed successfully")
        else:
            logger.warning(f"Data collection{type_desc} completed with errors")
            
    except Exception as e:
        logger.error(f"Error in data collection cycle{type_desc}: {e}", exc_info=True)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    scheduler.shutdown(wait=False)
    sys.exit(0)


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("Weather Station Data Collector")
    logger.info("=" * 60)
    logger.info(f"Configuration: {settings}")
    logger.info(f"Station: {settings.METEOBRIDGE_STATION_TAG}")
    logger.info(f"InfluxDB: {settings.INFLUXDB_URL}")
    logger.info("Collection intervals:")
    logger.info(f"  Temperature/Humidity: {settings.TEMP_HUMIDITY_INTERVAL}s")
    logger.info(f"  Wind: {settings.WIND_INTERVAL}s")
    logger.info(f"  Rain: {settings.RAIN_INTERVAL}s")
    logger.info(f"  Pressure: {settings.PRESSURE_INTERVAL}s")
    logger.info("=" * 60)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run once immediately for all sensors
    logger.info("Running initial data collection...")
    collect_and_store_data()
    
    # Map sensor types to their intervals
    sensor_intervals = {
        'temp_humidity': settings.TEMP_HUMIDITY_INTERVAL,
        'wind': settings.WIND_INTERVAL,
        'rain': settings.RAIN_INTERVAL,
        'pressure': settings.PRESSURE_INTERVAL,
    }
    
    # Schedule periodic collection for each sensor type
    for sensor_type, interval in sensor_intervals.items():
        scheduler.add_job(
            lambda st=sensor_type: collect_and_store_data(st),
            trigger=IntervalTrigger(seconds=interval),
            id=f'collect_{sensor_type}_data',
            name=f'Collect {sensor_type} data from Meteobridge',
            replace_existing=True
        )
        logger.info(f"Scheduled {sensor_type} collection every {interval}s")
    
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()