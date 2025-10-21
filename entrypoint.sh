#!/bin/bash
python -m scripts.setup_influxdb_tables
python main.py &
python rollup_main.py