import scrapy
from loguru import logger

class ToScrapeCSSSpider(scrapy.Spider):
    name = "toscrape-css"
    start_urls = [
        'https://www.binance.com/zh-CN/support/announcement/%E4%B8%8B%E6%9E%B6%E8%AE%AF%E6%81%AF?c=161&navId=161&hl=zh-CN',
    ]

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    def parse(self, response):
        # 使用 CSS 选择器选择链接、文本和时间
        # link_selector = 'div#app-wrap div.css-3ubhtt div.css-1qpfia7 div.css-14wab8k section.css-14d7djd div.css-148156o div.css-1q4wrpt div.css-1tl1y3y a::attr(href)'
        # text_selector = 'div#app-wrap div.css-3ubhtt div.css-1qpfia7 div.css-14wab8k section.css-14d7djd div.css-148156o div.css-1q4wrpt div.css-1tl1y3y a div.css-1yxx6id::text'
        # date_selector = 'div#app-wrap div.css-3ubhtt div.css-1qpfia7 div.css-14wab8k section.css-14d7djd div.css-148156o div.css-1q4wrpt div.css-1tl1y3y a div.css-1yxx6id h6::text'

        link_selector = 'div#app-wrap div.css-3ubhtt div.css-1q4wrpt a::attr(href)'

        text_selector = 'div#app-wrap::text'
        date_selector = 'div.css-rdrahp h6::text'




        links = response.css(link_selector).getall()
        texts = response.css(text_selector).getall()
        dates = response.css(date_selector).getall()

        logger.info(f"Number of links found: {len(links)}")
        logger.info(links)
        logger.info(f"Number of texts found: {len(texts)}")
        logger.info(texts)
        logger.info(f"Number of dates found: {len(dates)}")
        logger.info(dates)

        for link, text, date in zip(links, texts, dates):
            yield {
                'link': link,
                'text': text,
                'date': date,
            }

