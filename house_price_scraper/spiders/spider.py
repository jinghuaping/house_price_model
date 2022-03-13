import scrapy
# this refer to name of project
from house_price_scraper.items import HousePriceScraperItem, HousePriceScraperItemLoader
import csv
import os
import re
import logging
import random
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from scrapy import Request
from scrapy.utils.project import get_project_settings
from scrapy.linkextractors import LinkExtractor
from datetime import datetime
from math import ceil
import json


class HousePriceSpider(scrapy.Spider):
    def __init__(self):
        self.le = LinkExtractor(allow=r'^https://www.trulia.com/property')

    name = "house_price"
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
    }
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
    allowed_domains = ['trulia.com']
    #start_urls = ['https://www.trulia.com/sold/Saint_Louis,MO/']
    start_urls = ['https://www.trulia.com/MO/Saint_Louis/']
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:63.0) Gecko/20100101 Firefox/63.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:63.0) Gecko/20100101 Firefox/63.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36 Maxthon/5.1.5.3000",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1",
        "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
    ]

    def last_pagenumber_in_search(self, response):
        """ Returns the number of the last page on the CITY/locale page """
        resultsHtml = response.xpath('.//*/text()[contains(., " Results")]')
        try:
            self.logger.info(f'{str(resultsHtml)}')
            self.logger.info(resultsHtml[2].root[:-8]
                             [8:] + " results to scrape..")
            #number_of_results = int(resultsHtml[2].root.re_first(r'^1 - (\d+) of [\d,]+ Results$').replace(',', ''))
            number_of_results = int(
                resultsHtml[2].root[:-8][8:].replace(',', ''))
            self.logger.info('number of pages: %s', str(number_of_results))
            return ceil(number_of_results/30)
        except Exception as e:
            self.logger.info('Exception: %s', str(e))
            return 0

    def parse(self, response):
        #N = self.last_pagenumber_in_search(response)
        N = self.get_number_of_pages_to_scrape(response)
        self.logger.info(
            "Determined that property pages are contained on {N} different index pages, each containing at most 30 properties. Proceeding to scrape each index page...".format(N=N))
        for url in [response.urljoin("{n}_p/".format(n=n)) for n in range(1, N+1)]:
            yield scrapy.Request(url=url, callback=self.parse_index_page)

    def get_number_of_pages_to_scrape(self, response):
        pagination = response.xpath('.//*/text()[contains(., " Results")]')
        number_of_results = int(pagination.re_first(r'^1-40 of ([\d,]+) Results$').replace(',', ''))
        return ceil(number_of_results/40)

    def parse_index_page(self, response):
        self.logger.info(f'parse_index_page response: {str(response)}')
        for link in self.le.extract_links(response):
            yield scrapy.Request(url=link.url, callback=self.parse_property_page)

    def parse_property_page(self, response):
        l = HousePriceScraperItemLoader(item=HousePriceScraperItem(), response=response)
        self.logger.info(f'parse_property_page response: {str(response)}')
        self.load_common_fields(item_loader=l, response=response)

        listing_information = l.nested_xpath(
            '//span[text() = "LISTING INFORMATION"]')
        listing_information.add_xpath(
            'listing_information', './parent::div/following-sibling::ul[1]/li/text()')
        listing_information.add_xpath(
            'listing_information_date_updated', './following-sibling::span/text()', re=r'^Updated: (.*)')

        public_records = l.nested_xpath('//span[text() = "PUBLIC RECORDS"]')
        public_records.add_xpath(
            'public_records', './parent::div/following-sibling::ul[1]/li/text()')
        public_records.add_xpath('public_records_date_updated',
                                 './following-sibling::span/text()', re=r'^Updated: (.*)')

        item = l.load_item()
        self.post_process(item=item)
        return item

    @staticmethod
    def load_common_fields(item_loader, response):
        '''Load field values which are common to "on sale" and "recently sold" properties.'''
        item_loader.add_value('url', response.url)
        item_loader.add_xpath('address', '//*[@data-role="address"]/text()')
        item_loader.add_xpath(
            'city_state', '//*[@data-role="cityState"]/text()')
        item_loader.add_xpath(
            'price', '//span[@data-role="price"]/text()', re=r'\$([\d,]+)')
        item_loader.add_xpath(
            'neighborhood', '//*[@data-role="cityState"]/parent::h1/following-sibling::span/a/text()')
        details = item_loader.nested_css('.homeDetailsHeading')
        overview = details.nested_xpath(
            './/span[contains(text(), "Overview")]/parent::div/following-sibling::div[1]')
        overview.add_xpath('overview', xpath='.//li/text()')
        overview.add_xpath('area', xpath='.//li/text()', re=r'([\d,]+) sqft$')
        overview.add_xpath('lot_size', xpath='.//li/text()',
                           re=r'([\d,.]+) (?:acres|sqft) lot size$')
        overview.add_xpath('lot_size_units', xpath='.//li/text()',
                           re=r'[\d,.]+ (acres|sqft) lot size$')
        overview.add_xpath('price_per_square_foot',
                           xpath='.//li/text()', re=r'\$([\d,.]+)/sqft$')
        overview.add_xpath('bedrooms', xpath='.//li/text()',
                           re=r'(\d+) (?:Beds|Bed|beds|bed)$')
        overview.add_xpath('bathrooms', xpath='.//li/text()',
                           re=r'(\d+) (?:Baths|Bath|baths|bath)$')
        overview.add_xpath(
            'year_built', xpath='.//li/text()', re=r'Built in (\d+)')
        overview.add_xpath('days_on_Trulia', xpath='.//li/text()',
                           re=r'([\d,]) days on Trulia$')
        overview.add_xpath('views', xpath='.//li/text()',
                           re=r'([\d,]+) views$')
        item_loader.add_css('description', '#descriptionContainer *::text')

        price_events = details.nested_xpath(
            './/*[text() = "Price History"]/parent::*/following-sibling::*[1]/div/div')
        price_events.add_xpath('prices', './div[contains(text(), "$")]/text()')
        price_events.add_xpath(
            'dates', './div[contains(text(), "$")]/preceding-sibling::div/text()')
        price_events.add_xpath(
            'events', './div[contains(text(), "$")]/following-sibling::div/text()')

    @staticmethod
    def post_process(item):
        '''Add any additional data to an item after loading it'''
        if item.get('dates') is not None:
            dates = [datetime.datetime.strptime(
                date, '%m/%d/%Y') for date in item['dates']]
            prices = [int(price.lstrip('$').replace(',', ''))
                      for price in item['prices']]
            item['price_history'] = sorted(
                list(zip(dates, prices, item['events'])), key=lambda x: x[0])


# main driver #
if __name__ == "__main__":
    # Create Instance called 'cl' as in "c"raigs "l"ist
    now = datetime.now()  # current date and time
    date_time = now.strftime("%Y-%m-%d_%H_%M")
    file_name = f'results_{date_time}_main.csv'

    s = get_project_settings()
    s['FEED_FORMAT'] = 'csv'
    s['LOG_LEVEL'] = 'INFO'
    #s['FEED_URI'] = file_name
    s['LOG_FILE'] = 'scrapy.log'
    """
    cl = CrawlerProcess(settings={
        "FEEDS": {
            "results.csv": {"format": "csv"},
        },
    })
    """
    cl = CrawlerProcess(s)

    cl.crawl(HousePriceSpider)
    cl.start()
