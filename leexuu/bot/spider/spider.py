import os
import scrapy
import json
from urllib.parse import quote
from scrapy.crawler import CrawlerProcess
from loguru import logger



class Spider(scrapy.Spider):
    name = "Spider"

    allowed_domains = ['bitget.com']

    urls = {
        # 'binance': 'https://www.binance.com/zh-CN/support/announcement/%E4%B8%8B%E6%9E%B6%E8%AE%AF%E6%81%AF?c=161&navId=161&hl=zh-CN',
        # 'okx': 'https://www.okx.com/zh-hans/help/section/announcements-latest-announcements',
        # 'okx_announcements-api': 'https://www.okx.com/zh-hans/help/section/announcements-api',
        'okx': 'https://www.okx.com/zh-hans/help/section/announcements-api', #未爬取
        # 'gate': 'https://www.gate.io/zh/announcements',
        # 'bitget': 'https://www.bitget.com/zh-CN/support/categories/11865590960081', #未爬取
        'bitget': 'https://www.bitget.com/zh-CN/academy'
        # 'bybit': 'https://announcements.bybit.com/zh-MY/?page=1&category=new_crypto',
        # 'bybit_delist': 'https://announcements.bybit.com/zh-MY/?page=1&category=delistings',
    }

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 3,  # Add a delay to avoid detection
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
        },
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [403, 429],
        'HTTPERROR_ALLOWED_CODES': [403],

    }
    

    # 用于存储公告URL的字典，避免重复
    article_urls = {'binance': {}, 'okx': {}, 'okx_announcements-api': {}, 'gate': {}, 'bitget': {}, 'bybit': {}, 'bybit_delist': {}}

    def start_requests(self):
        for exchange, url in self.urls.items():
            yield scrapy.Request(url=url, callback=self.parse, meta={'exchange': exchange}, headers={
                'Referer': 'https://www.google.com/',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Connection': 'keep-alive',
            })

    def parse(self, response):
        exchange = response.meta['exchange']

        if exchange == 'bitget':
            app_data_selector = 'div.ArticleList_actice_main__qlrgF'
            app_data_text = response.css(app_data_selector).extract()
            logger.info(app_data_text)
            if not app_data_text:
                logger.error("No data found")
                return

        if exchange == 'bybit_delist':
            app_data_selector = 'div.article-item'
            app_data_text = response.css(app_data_selector)
            if not app_data_text:
                logger.error("No data found")
                return
            
            for idx, app_data in enumerate(app_data_text):
                article_title = app_data.css('.article-item-title span::text').get()
                article_url = response.urljoin(app_data.css(f'{app_data_selector}[data-cy="announcement-{idx}"]::attr(href)').get())
                if article_title and article_url:
                    if article_title not in self.article_urls['bybit_delist']:
                        self.article_urls['bybit_delist'][article_title] = article_url
                        self.logger.info(f"Bybit: New article URL: {article_url}")
                    else:
                        self.logger.info(f"Bybit: Duplicate article found: {article_title}, skipping")
                else:
                    logger.error(f"Missing title or URL in article: {app_data}")
           
        if exchange == 'bybit':
            app_data_selector = 'div.article-item'
            app_data_text = response.css(app_data_selector)
            if not app_data_text:
                logger.error("No data found")
                return
            for idx, app_data in enumerate(app_data_text):
                article_title = app_data.css('.article-item-title span::text').get()
                article_url = response.urljoin(app_data.css(f'{app_data_selector}[data-cy="announcement-{idx}"]::attr(href)').get())
                if article_title and article_url:
                    if article_title not in self.article_urls['bybit']:
                        self.article_urls['bybit'][article_title] = article_url
                        # self.logger.info(f"Bybit: New article URL: {article_url}")
                    else:
                        self.logger.info(f"Bybit: Duplicate article found: {article_title}, skipping")
                else:
                    logger.error(f"Missing title or URL in article: {app_data}")

        if exchange == 'gate':
            articles = response.css('div.article-list-item')
            if not articles:
                logger.error(f"No articles found for {exchange}")
                return
            for article in articles:
                try:
                    article_url = response.urljoin(article.css('a::attr(href)').get())
                    article_title = article.css('h3 span::text').get()
                    if article_title and article_url:
                        if article_title not in self.article_urls['gate']:
                            self.article_urls['gate'][article_title] = article_url
                            # logger.info(f"Gate: New article URL: {article_url}")
                        else:
                            logger.info(f"Gate: Duplicate article found: {article_title}, skipping")
                    else:
                        logger.error(f"Missing title or URL in article: {article}")
                except Exception as e:
                    logger.error(f"Error parsing article: {article}. Error: {e}")

            # logger.info(f"Gate: Total unique articles: {len(self.article_urls['gate'])}")
            # logger.info(f"Gate: Article URLs: {self.article_urls['gate']}")

        if exchange == 'okx_announcements-api':
            app_data_selector = 'li.index_article__15dX1'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            
            for article_html in app_data_text:
                article_url = response.urljoin(article_html.split('href="')[1].split('"')[0])
                article_title = article_html.split('index_title__6wUnB">')[1].split('</div>')[0]
                
                if article_title not in self.article_urls['okx_announcements-api']:
                    self.article_urls['okx_announcements-api'][article_title] = article_url
                    # logger.info(f"OKX: New article URL: {article_url}")
                else:
                    logger.info(f"OKX: Duplicate article found: {article_title}, skipping")

            # logger.info(f"OKX: Total unique articles: {len(self.article_urls['okx_announcements-api'])}")
            # logger.info(f"OKX: Article URLs: {self.article_urls['okx_announcements-api']}")
                    
        if exchange == 'okx':
            app_data_selector = 'li.index_article__15dX1'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            
            for article_html in app_data_text:
                article_url = response.urljoin(article_html.split('href="')[1].split('"')[0])
                article_title = article_html.split('index_title__6wUnB">')[1].split('</div>')[0]
                
                if article_title not in self.article_urls['okx']:
                    self.article_urls['okx'][article_title] = article_url
                    # logger.info(f"OKX: New article URL: {article_url}")
                else:
                    logger.info(f"OKX: Duplicate article found: {article_title}, skipping")

            # logger.info(f"OKX: Total unique articles: {len(self.article_urls['okx'])}")
            # logger.info(f"OKX: Article URLs: {self.article_urls['okx']}")

        if exchange == 'binance':
            app_data_selector = '//*[@id="__APP_DATA"]/text()'
            app_data_text = response.xpath(app_data_selector).get()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            try:
                data = json.loads(app_data_text)
                catalogs = data['appState']['loader']['dataByRouteId']['d9b2']['catalogs']
                # logger.info(f"{exchange}: Catalogs found: {len(catalogs)}")
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing JSON for {exchange}: {e}")
                return
            valid_catalog_ids = {48, 49, 51, 161}
            for catalog in catalogs:
                catalog_id = catalog.get('catalogId')
                if catalog_id not in valid_catalog_ids:
                    continue
                articles = catalog.get('articles', [])
                # logger.info(f"{exchange}: Parsing catalogId: {catalog_id} with {len(articles)} articles")
                for article in articles:
                    article_code = article['code']
                    if article_code not in self.article_urls[exchange]:
                        title_encoded = quote(article['title']).replace(' ', '')
                        article_url = f"https://www.{exchange}.com/zh-CN/support/announcement/{title_encoded}-{article_code}"
                        self.article_urls[exchange][article['title']] = article_url
                        # logger.info(f"{exchange}: New article URL: {article_url}")
                    else:
                        logger.info(f"{exchange}: Duplicate article found: {article_code}, skipping")

        # 打印或保存 article_urls 字典
        logger.info(f"{exchange}: Total unique articles: {len(self.article_urls[exchange])}")
        logger.info(f"{exchange}: Article URLs: {self.article_urls[exchange]}")


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(Spider)
    process.start()



