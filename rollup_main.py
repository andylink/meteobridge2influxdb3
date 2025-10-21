from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from services.rollup_service import WeatherRollupService
from utils import setup_logger

logger = setup_logger('rollup_main')
scheduler = BlockingScheduler()

def run_hourly_rollup():
    logger.info("Starting hourly rollup job")
    service = WeatherRollupService()
    service.compute_hourly_rollup()

def run_daily_rollup():
    logger.info("Starting daily rollup job")
    service = WeatherRollupService()
    service.compute_daily_rollup()

def main():
    # Hourly rollup: 5 minutes past each hour
    scheduler.add_job(
        run_hourly_rollup,
        trigger=CronTrigger(minute=5),
        id='hourly_rollup',
        name='Hourly rollup at 15 minutes past each hour',
        replace_existing=True
    )
    # Daily rollup: 00:10 UTC
    scheduler.add_job(
        run_daily_rollup,
        trigger=CronTrigger(hour=0, minute=10),
        id='daily_rollup',
        name='Daily rollup at 00:10 UTC',
        replace_existing=True
    )
    logger.info("Rollup scheduler started")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Rollup scheduler stopped.")

if __name__ == "__main__":
    main()