import os
import scrapy
import json
import re
from urllib.parse import quote
from scrapy.crawler import CrawlerProcess
from scrapy import Request
from loguru import logger


class BinanceSpider(scrapy.Spider):
    name = "binance"
    start_urls = [
        'https://www.binance.com/zh-CN/support/announcement/%E4%B8%8B%E6%9E%B6%E8%AE%AF%E6%81%AF?c=161&navId=161&hl=zh-CN',
    ]

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    def parse(self, response):
        app_data_selector = '//*[@id="__APP_DATA"]/text()'
        app_data_text = response.xpath(app_data_selector).get()
        
        if not app_data_text:
            logger.error("No data found")
            return

        try:
            data = json.loads(app_data_text)
            catalogs = data['appState']['loader']['dataByRouteId']['d9b2']['catalogs']
            logger.info(catalogs)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing JSON: {e}")
            return

        for catalog in catalogs:
            if catalog['catalogId'] == 48:
                articles = catalog['articles']
                for article in articles:
                    title_encoded = quote(article['title']).replace(' ', '')
                    article_url = f"https://www.binance.com/zh-CN/support/announcement/{title_encoded}-{article['code']}"
                    logger.info(article_url)
                    yield Request(article_url, callback=self.parse_article, meta={'article': article})
                    break

    def parse_article(self, response):
        article = response.meta['article']
        app_data_selector = '//*[@id="__APP_DATA"]/text()'
        app_data_text = response.xpath(app_data_selector).get()
        
        if not app_data_text:
            logger.error("No article data found")
            return

        try:
            data = json.loads(app_data_text)
            article_content = data['pageData']['shuviInitialState']['global']['metaData']['description']
            logger.info(article_content)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing article JSON: {e}")
            return
        
        # 移除HTML标签和样式
        article_text = re.sub(r'(?i)<style[^>]*>(.*?)</style>', '', article_content, flags=re.DOTALL)
        article_text = re.sub(r'<[^>]+>', '', article_text)
        logger.info(article_text)


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(BinanceSpider)
    process.start()
