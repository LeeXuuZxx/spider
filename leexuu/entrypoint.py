import schedule
import time
from scrapy.cmdline import execute
from loguru import logger

def run_spider():
    execute(['scrapy', 'crawl', 'Spider'])

# Schedule the spider to run every hour
schedule.every(1).hour.do(run_spider)

# Alternatively, you can schedule the spider to run at specific intervals, like every 10 minutes
# schedule.every(10).minutes.do(run_spider)

if __name__ == "__main__":
    logger.info("Script started")

    run_spider()  # Run the spider initially when the script starts
    logger.info("Spider completed")

    while True:
        schedule.run_pending()
        time.sleep(1)
