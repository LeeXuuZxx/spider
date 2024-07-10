import os
import scrapy
import json
from urllib.parse import quote
from scrapy.crawler import CrawlerProcess
from loguru import logger
import requests
from bs4 import BeautifulSoup
import csv
import time
import aiohttp
import asyncio
from enum import Enum

class TopicEnum(Enum):
    ANNOUNCEMENT = 131203  # 公告

class Spider(scrapy.Spider):
    name = "Spider"

    urls = {
        'binance': 'https://www.binance.com/zh-CN/support/announcement/%E4%B8%8B%E6%9E%B6%E8%AE%AF%E6%81%AF?c=161&navId=161&hl=zh-CN',
        'binance_api': 'https://developers.binance.com/docs/zh-CN/binance-spot-api-docs/CHANGELOG',
        'okx': 'https://www.okx.com/zh-hans/help/section/announcements-latest-announcements',
        'okx_announcements-api': 'https://www.okx.com/zh-hans/help/section/announcements-api',
        'okx_api': 'https://www.okx.com/docs-v5/log_zh/#upcoming-changes-copy-trading-restriction-fucntion',
        'gate': 'https://www.gate.io/zh/announcements',
        'gate_api': 'https://www.gate.io/docs/developers/apiv4/zh_CN/#%E6%8A%80%E6%9C%AF%E6%94%AF%E6%8C%81',
        'bitget': 'https://api.bitget.com/api/v2/public/annoucements',
        'bybit': 'https://announcements.bybit.com/zh-MY/?page=1&category=new_crypto',
        'bybit_delist': 'https://announcements.bybit.com/zh-MY/?page=1&category=delistings',
        'bybit_api': 'https://bybit-exchange.github.io/docs/zh-TW/changelog/v5',
    }

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 3,  # Add a delay to avoid detection
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        # 'DOWNLOADER_MIDDLEWARES': {
        #     'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
        #     'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        #     'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
        #     'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
        # },
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [403, 429],
        'HTTPERROR_ALLOWED_CODES': [403],
    }
    
    # TELEGRAM_BOT_TOKEN = '7283290474:AAHnBvxcxlYqQFBa4r2RjykTee8H6eQAgSQ'
    # TELEGRAM_CHAT_ID = '2115436972'
    TELEGRAM_BOT_TOKEN = '6752350542:AAEaBk2n3rImEWEhCBMwIbBU1Wq01zkN_54'
    TELEGRAM_CHAT_ID = '-1001958551048'
    

    # 用于存储公告URL的字典，避免重复
    article_urls = {'binance': {}, 'binance_api': {}, 'okx': {}, 'okx_announcements-api': {}, 'okx_api': {}, 'gate': {}, 'gate_api': {}, 'bitget': {}, 'bybit': {}, 'bybit_delist': {}, 'bybit_api': {}}

    sleep_time = 3600
    count_times = 0
    def start_requests(self):
        for exchange, url in self.urls.items():
            # logger.info("开始爬取")
            if exchange == 'bitget':
                # logger.info(f"正在爬取 {exchange} 公告")
                self.bitget(url)
                # logger.info(f"已爬取 {exchange} 公告")
                # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")
                # logger.info(f"当前时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
            else:
                # logger.info(f"正在爬取 {exchange} 公告")
                yield scrapy.Request(url=url, callback=self.parse, meta={'exchange': exchange}, headers={
                    'Referer': 'https://www.google.com/',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Connection': 'keep-alive',
                })
                # logger.info(f"已爬取 {exchange} 公告")
        self.count_times += 1
        logger.info(f"已爬取 {self.count_times} 次")
        logger.info(f"当前时间：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

    def bitget(self, url):
        params = {'language': 'zh_CN'}
        try:
            exchange = 'bitget'
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                announcements = data.get('data', [])
                for announcement in announcements:
                    title = announcement.get('annTitle', '')
                    if title not in self.article_urls['bitget']:
                        self.article_urls['bitget'][title] = announcement.get('annUrl', '')
            else:
                print(f"Request failed with status code {response.status_code}")

            # 打印或保存 article_urls 字典
            # logger.info(f"{exchange}: Total unique articles: {len(self.article_urls[exchange])}")
            # logger.info(f"{exchange}: Article URLs: {self.article_urls[exchange]}")
            
            asyncio.run(self.save_to_csv())


        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")

    def check_loop_start(self):
        if self.is_finished():  # 当队列中没有请求时休眠并重启
            self.logger.info('本轮采集完毕，%s秒后开始下一轮采集' % self.sleep_time)
            time.sleep(self.sleep_time)
            return self.start_requests()
        return []
    
    def is_finished(self):  # 检查队列中是否还有请求
        if self.crawler.engine.downloader.active:
            return False
        if self.crawler.engine.slot.start_requests is not None:
            return False
        if self.crawler.engine.slot.scheduler.has_pending_requests():
            return False
        return True

    def parse(self, response):
        exchange = response.meta['exchange']

        if exchange == 'bybit_api':
            app_data_selector = 'div.row'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            soup = BeautifulSoup(app_data_text, 'html.parser')
            
            base_url = 'https://bybit-exchange.github.io/docs/zh-TW/changelog/v5'
            
            for h2_tag in soup.find_all('h2'):
                current_date = h2_tag.get_text(strip=True)
                # logger.info(current_date)
                
                sibling = h2_tag.find_next_sibling()
                while sibling and sibling.name != 'h2':
                    if sibling.name == 'h3':
                        section = sibling.get_text(strip=True)
                        # logger.info(section)
                        ul_tag = sibling.find_next('ul')
                        if ul_tag:
                            for li_tag in ul_tag.find_all('li'):
                                title_content = li_tag.get_text(strip=True)
                                a_tag = li_tag.find('a', href=True)
                                if a_tag:
                                    title = f"{current_date} {section} {title_content}"
                                    url = base_url
                                    if title not in self.article_urls[exchange]:
                                        self.article_urls[exchange][title] = url
                    sibling = sibling.find_next_sibling()
            # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")

        if exchange == 'gate_api':
            app_data_selector = 'div.content-block__cont'
            app_data_text = response.css(app_data_selector).extract()
            # logger.info(app_data_text)

            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            try:
                soup = BeautifulSoup(app_data_text, 'html.parser')
                current_date = None
                for p_tag in soup.find_all('p'):
                    strong_tag = p_tag.find('strong')
                    if strong_tag:
                        version = p_tag.get_text(strip=True)
                        date_tag = p_tag.find_next_sibling('p')
                        if date_tag:
                            current_date = date_tag.get_text(strip=True)
                        ul_tag = p_tag.find_next('ul')
                        if ul_tag:
                            for li_tag in ul_tag.find_all('li'):
                                title = f"{current_date} {li_tag.get_text(strip=True)}"
                                url = 'https://www.gate.io/docs/developers/apiv4/zh_CN/#%E6%8A%80%E6%9C%AF%E6%94%AF%E6%8C%81'
                                if title not in self.article_urls['gate_api']:
                                    self.article_urls['gate_api'][title] = url
                # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")

                # logger.info(self.article_urls['gate_api'])

            except Exception as e:
                logger.error(f"Error parsing HTML: {e}")

        if exchange == 'binance_api':
            app_data_selector = 'div.markdown'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            # 将列表中的HTML内容合并为一个字符串
            app_data_text = ''.join(app_data_text)
            try:
                # 使用BeautifulSoup解析HTML
                soup = BeautifulSoup(app_data_text, 'html.parser')
                # 初始化变量来存储当前日期和标题信息
                current_date = None
                first_date_found = False
                # 找到所有的<h1>标签和<ul>标签
                for element in soup.find_all(['h1', 'ul']):
                    if element.name == 'h1':
                        if not first_date_found:
                            current_date = element.get_text(strip=True)
                            # logger.info(f"Current date: {current_date}")
                            first_date_found = True
                        else:
                            break
                        if current_date not in self.article_urls['binance_api']:
                            title = current_date
                            url = 'https://developers.binance.com/docs/zh-CN/binance-spot-api-docs/CHANGELOG'
                            if title not in self.article_urls['binance_api']:
                                self.article_urls['binance_api'][title] = url
                # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")
                
                # 打印或保存 article_urls 字典
                # logger.info(f"binance_api: Total unique articles: {len(self.article_urls['binance_api'])}")
                # logger.info(f"binance_api: Article URLs: {self.article_urls['binance_api']}")
            except Exception as e:
                logger.error(f"Error parsing HTML: {e}")

        if exchange == 'okx_api':
            app_data_selector = 'div.content'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            soup = BeautifulSoup(app_data_text, 'html.parser')
            base_url = 'https://www.okx.com'
            
            h1_tags = soup.find_all('h1')
            for h1_tag in h1_tags:
                current_date = h1_tag.get_text(strip=True)
                ul_tag = h1_tag.find_next('ul')
                if ul_tag:
                    for li_tag in ul_tag.find_all('li'):
                        title_content = li_tag.get_text(strip=True)
                        a_tag = li_tag.find('a', href=True)
                        if a_tag:
                            title = f"{current_date} {title_content}"
                            url = base_url + a_tag['href']
                            if title not in self.article_urls['okx_api']:
                                self.article_urls['okx_api'][title] = url
                                time.sleep(1)

            # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")

        if exchange == 'okx':
            app_data_selector = 'div.home-container'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            soup = BeautifulSoup(app_data_text, 'html.parser')
            base_url = 'https://www.okx.com'
            announcements = soup.select('a.index_articleItem__d-8iK') #这里是公告对应的index_articleItem，可能会更换
            for announcement in announcements:
                title = announcement.select_one('div.index_articleTitle__ys7G7').get_text(strip=True) #这里是公告对应的index_articleTitle，可能会更换
                url = base_url + announcement['href']
                # logger.info(f"Title: {title}, URL: {url}")

                if title not in self.article_urls[exchange]:
                    self.article_urls[exchange][title] = url

            # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")
                    
        if exchange == 'okx_announcements-api':
            app_data_selector = 'div.home-container'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            soup = BeautifulSoup(app_data_text, 'html.parser')
            base_url = 'https://www.okx.com'
            
            announcements = soup.select('a.index_articleItem__d-8iK')
            for announcement in announcements:
                title_tag = announcement.select_one('div.index_articleTitle__ys7G7')
                date_tags = announcement.select('span[data-testid="DateDisplay"]')
                if title_tag and len(date_tags) >= 2:
                    title = title_tag.get_text(strip=True)
                    published_date = date_tags[0].get_text(strip=True)
                    updated_date = date_tags[1].get_text(strip=True) if len(date_tags) > 1 else ""
                    full_title = f"{title}发布于 {published_date}更新于 {updated_date}"
                    url = base_url + announcement['href']
                    # logger.info(f"Title: {full_title}, URL: {url}")
                    if full_title not in self.article_urls['okx_announcements-api']:
                        self.article_urls['okx_announcements-api'][full_title] = url
                        time.sleep(1)

            # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")

        if exchange == 'bybit_delist':
            app_data_selector = 'div.app'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            soup = BeautifulSoup(app_data_text, 'html.parser')
            base_url = 'https://announcements.bybit.com'
            announcements = soup.select('a.no-style')
            for announcement in announcements:
                title_tag = announcement.select_one('div.article-item-title span')
                date_tag = announcement.select_one('div.article-item-date')
                if title_tag and date_tag:
                    title = title_tag.get_text(strip=True)
                    published_date = date_tag.get_text(strip=True)
                    full_title = f"{title}发布于 {published_date}"
                    relative_url = announcement['href']
                    url = f"{base_url}{relative_url}"
                    # logger.info(f"Title: {full_title}, URL: {url}")
                    if full_title not in self.article_urls['bybit_delist']:
                        self.article_urls['bybit_delist'][full_title] = url
                        time.sleep(1)
            # logger.info(f"当前交易所 {exchange} 爬取公告数量：{len(announcements)}")
           
        if exchange == 'bybit':
            app_data_selector = 'div.app'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            soup = BeautifulSoup(response.text, 'html.parser')
            base_url = 'https://announcements.bybit.com'
            announcements = soup.select('a.no-style')
            for announcement in announcements:
                title_tag = announcement.select_one('div.article-item-title span')
                date_tag = announcement.select_one('div.article-item-date')
                if title_tag and date_tag:
                    title = title_tag.get_text(strip=True)
                    published_date = date_tag.get_text(strip=True)
                    full_title = f"{title}发布于 {published_date}"
                    relative_url = announcement['href']
                    url = f"{base_url}{relative_url}"
                    # logger.info(f"Title: {full_title}, URL: {url}")
                    if full_title not in self.article_urls['bybit']:
                        self.article_urls['bybit'][full_title] = url
                        time.sleep(1)
            # logger.info(f"当前交易所 {exchange} 爬取公告数量：{len(announcements)}")
           
        if exchange == 'gate':
            app_data_selector = 'div.article-list-box'
            app_data_text = response.css(app_data_selector).extract()
            if not app_data_text:
                logger.error(f"No data found for {exchange}")
                return
            app_data_text = ''.join(app_data_text)
            soup = BeautifulSoup(response.text, 'html.parser')
            base_url = 'https://www.gate.io'
            announcements = soup.select('div.article-list-item')

            for announcement in announcements:
                title_tag = announcement.select_one('a.article-list-item-title h3 span')
                date_tag = announcement.select_one('span.article-list-info-timer span')
                if title_tag and date_tag:
                    title = title_tag.get_text(strip=True)
                    published_date = date_tag.get_text(strip=True)
                    full_title = f"{title}发布于 {published_date}"
                    relative_url = announcement.select_one('a.article-list-item-title')['href']
                    url = f"{base_url}{relative_url}"
                    # logger.info(f"Title: {full_title}, URL: {url}")
                    if full_title not in self.article_urls['gate']:
                        self.article_urls['gate'][full_title] = url
                        time.sleep(1)
            # logger.info(f"当前交易所 {exchange} 爬取公告数量：{len(announcements)}")

        if exchange == 'binance': #权限不够，只能拿__APP_DATA
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
                    # else:
                    #     logger.info(f"{exchange}: Duplicate article found: {article_code}, skipping")
            # logger.info(f"当前交易所{exchange}爬取公告数量：{len(self.article_urls[exchange])}")
        
        asyncio.run(self.save_to_csv())

        for request in self.check_loop_start():
            yield request


        # 打印或保存 article_urls 字典
        # logger.info(f"{exchange}: Total unique articles: {len(self.article_urls[exchange])}")
        # logger.info(f"{exchange}: Article URLs: {self.article_urls[exchange]}")
    async def send_telegram_message(self, message, topic_enum: TopicEnum):
        url = f"https://api.telegram.org/bot{self.TELEGRAM_BOT_TOKEN}/sendMessage"
        reply_to_message_id = topic_enum.value
        data = {
            "chat_id": self.TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "reply_to_message_id": reply_to_message_id
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    response_text = await response.text()
                    response.raise_for_status()
        except aiohttp.ClientError as e:
            logger.error(f"Error sending message to Telegram: {e}")

    async def save_to_csv(self):
        try:
            # logger.info("正在保存数据到 CSV 文件...")
            for exchange, articles in self.article_urls.items():
                file_path = f'{exchange}.csv'
                existing_titles = set()
                if os.path.exists(file_path):
                    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                        reader = csv.DictReader(csvfile)
                        for row in reader:
                            existing_titles.add(row['Title'])
                with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['Title', 'URL']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    new_articles_count = 0
                    new_articles = []
                    for title, url in articles.items():
                        if title not in existing_titles:
                            writer.writerow({'Title': title, 'URL': url})
                            new_articles.append({'Title': title, 'URL': url})
                            new_articles_count += 1
                    for article in new_articles:
                        message = f"New announcement from {exchange}:\nTitle: {article['Title']}\nURL: {article['URL']}"
                        await self.send_telegram_message(message, topic_enum=TopicEnum.ANNOUNCEMENT)
                        time.sleep(0.5)
                        logger.info(message)
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
    

    def next_parse(self, response):
        for request in self.check_loop_start():
            yield request




# if __name__ == "__main__":
#     logger.info("正在启动爬虫...")
#     while True: #没有一直循环,bitget的公告没有完全写入就停了
#         process = CrawlerProcess()
#         process.crawl(Spider)
#         process.start()


# async def start_crawler():
#     process = CrawlerProcess()
#     process.crawl(Spider)
#     process.start()

# if __name__ == "__main__":
#     logger.info("正在启动爬虫...")
#     loop = asyncio.get_event_loop()
#     while True:
#         loop.run_until_complete(start_crawler())
#         time.sleep(300)  # wait 5 minutes before next run



