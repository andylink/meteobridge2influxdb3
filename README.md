# Weather Station Data Collector

Collects weather data from a Meteobridge personal weather station and stores it in InfluxDB at regular intervals.

## Features

- 🌤️ Fetches real-time weather data from Meteobridge
- 📊 Stores data in InfluxDB time-series database
- ⏱️ Configurable collection intervals per sensor type
- 🔧 Environment-based configuration
- 📝 Comprehensive logging
- 🛡️ Graceful shutdown handling

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

## Configuration

Edit the `.env` file with your settings:

- **Meteobridge**: IP address, credentials, timezone
- **InfluxDB**: URL, token, organization, bucket  
- **Collection Intervals**: Configure different frequencies for sensor types:
  - `TEMP_HUMIDITY_INTERVAL`: Temperature and humidity readings (default: 60s)
  - `WIND_INTERVAL`: Wind measurements (default: 10s) 
  - `RAIN_INTERVAL`: Rain measurements (default: 60s)
  - `PRESSURE_INTERVAL`: Pressure measurements (default: 60s)
  - `COLLECTION_INTERVAL`: Legacy setting for backward compatibility (default: 10s)

## Usage

Run the collector:
```bash
python main.py
```

The collector will:
1. Run an initial data collection immediately for all sensors
2. Schedule separate periodic collections for each sensor type based on their configured intervals
3. Continue running until stopped with Ctrl+C

### Collection Frequencies

By default, the system collects:
- **Temperature & Humidity**: Every 60 seconds
- **Wind Data**: Every 10 seconds (for better wind tracking)
- **Rain Data**: Every 60 seconds  
- **Pressure Data**: Every 60 seconds

## Running as a Service

### systemd (Linux)

Create `/etc/systemd/system/weather-collector.service`:

```ini
[Unit]
Description=Weather Station Data Collector
After=network.target influxdb.service

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/weather_collector
Environment="PATH=/path/to/weather_collector/venv/bin"
ExecStart=/path/to/weather_collector/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable weather-collector
sudo systemctl start weather-collector
sudo systemctl status weather-collector
```

## Project Structure

```
weather_collector/
├── config/          # Configuration management
├── services/        # External service clients
├── models/          # Data models
├── utils/           # Utilities (logging, etc.)
└── main.py          # Application entry point
```

## Logging

Logs are written to stdout with configurable levels (DEBUG, INFO, WARNING, ERROR).
Set `LOG_LEVEL` in `.env` to control verbosity.

## License

MIT