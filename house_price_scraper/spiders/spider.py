import scrapy
from house_price_scraper.items import HousePriceScraperItem  # this refer to name of project
import csv
import os
import logging
import random
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from scrapy import Request
from scrapy.utils.project import get_project_settings
from datetime import datetime


class CraigslistItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    hood = scrapy.Field()
    link = scrapy.Field()
    misc = scrapy.Field()
    lon = scrapy.Field()
    lat = scrapy.Field()


class HousePriceSpider(scrapy.Spider):
    try:
        now = datetime.now()  # current date and time
        date_time = now.strftime("%Y-%m-%d_%H_%M")
        file_name = f'results_{date_time}.csv'
        os.remove(file_name)
    except OSError:
        pass

    def __init__(self):
        self.lat = ""
        self.lon = ""

    name = "house_price"
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
    }
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"
    # allowed_domains = ['propertyguru.com.sg']
    """
    start_urls = [
        r'http://www.propertyguru.com.sg/simple-listing/property-for-sale?market=residential&property_type_code%5B%5D=4A&property_type_code%5B%5D=4NG&property_type_code%5B%5D=4S&property_type_code%5B%5D=4I&property_type_code%5B%5D=4STD&property_type=H&freetext=Jurong+East%2C+Jurong+West&hdb_estate%5B%5D=13&hdb_estate%5B%5D=14'
    ]
    start_urls = ["https://in.finance.yahoo.com/quote/MSFT?p=MSFT",
                  "https://in.finance.yahoo.com/quote/MSFT/key-statistics?p=MSFT",
                  "https://in.finance.yahoo.com/quote/MSFT/holders?p=MSFT"]
    """
    start_urls = ['http://stlouis.craigslist.org/d/real-estate/search/rea/']
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

    """
    def start_requests(self):
        for url in self.start_urls:
            #user_agent = random.choice(self.user_agent_list)
            #self.headers["user-agent"] = user_agent
            yield scrapy.Request(url, headers=self.headers)
    """

    def start_requests(self):
        user_agent = random.choice(self.user_agent_list)
        self.headers["user-agent"] = user_agent
        yield scrapy.Request('http://stlouis.craigslist.org/d/real-estate/search/rea/', callback=self.parse,
                             headers=self.headers)

    def parse(self, response):

        all_ads = response.xpath('//div[@class="result-info"]')
        for ads in all_ads:
            price = ads.xpath(".//span[@class='result-price']/text()").get()
            date = ads.xpath(".//time[@class='result-date']/text()").get()
            title = ads.xpath(".//a[@class='result-title hdrlnk']/text()").get()
            hood = ads.xpath(".//span[@class='result-hood']/text()").get()
            details_link = ads.xpath(".//a[@class='result-title hdrlnk']/@href").get()

            # call parse_details and pass all of the above to it
            request = Request(url=details_link, callback=self.parse_detail, cb_kwargs={
                'price': price,
                'date': date,
                'title': title,
                'hood': hood,
                'details_link': details_link
            })

            yield request

        # Get the next 25 properties from 'next page' - persist until no more #
        next_page = response.xpath("//a[@class='button next']/@href").get()
        if next_page:
            yield response.follow(url=next_page, callback=self.parse)

    def parse_detail(self, response, price, date, title, hood, details_link):

        lon = response.xpath('//meta[@name="geo.position"]/@content').get().split(";")[0]
        lat = response.xpath('//meta[@name="geo.position"]/@content').get().split(";")[1]

        yield {
            'price': price,
            'date': date,
            'title': title,
            'hood': hood,
            'details_link': details_link,
            'lon': lon,
            'lat': lat
        }

    """
    def parse(self, response):
        filename = response.url.split("/")[-2] + '.html'
        print('filename', filename)

        with open(filename, 'wb') as f:
            f.write(response.body)
    """

    """
    # Parsing function
    def parse(self, response):

        # Using xpath to extract all the table rows
        data = response.xpath('//div[@id="quote-summary"]/div/table/tbody/tr')

        # If data is not empty
        if data:

            # Extracting all the text within HTML tags
            values = data.css('*::text').getall()

            # CSV Filename
            filename = 'quote.csv'

            # If data to be written is not empty
            if len(values) != 0:

                # Open the CSV File
                with open(filename, 'a+', newline='') as file:

                    # Writing in the CSV file
                    f = csv.writer(file)
                    for i in range(0, len(values[:24]), 2):
                        f.writerow([values[i], values[i + 1]])

        # Using xpath to extract all the table rows
        data = response.xpath('//section[@data-test="qsp-statistics"]//table/tbody/tr')

        if data:

            # Extracting all the table names
            values = data.css('span::text').getall()

            # Extracting all the table values
            values1 = data.css('td::text').getall()

            # Cleaning the received vales
            values1 = [value for value in values1 if value != ' ' and (value[0] != '(' or value[-1] != ')')]

            # Opening and writing in a CSV file
            filename = 'stats.csv'

            if len(values) != 0:
                with open(filename, 'a+', newline='') as file:
                    f = csv.writer(file)
                    for i in range(9):
                        f.writerow([values[i], values1[i]])

        # Using xpath to extract all the table rows
        data = response.xpath('//div[@data-test="holder-summary"]//table')
        print('data:', data)

        if data:
            # Extracting all the table names
            values = data.css('span::text').getall()

            # Extracting all the table values
            values1 = data.css('td::text').getall()

            # Opening and writing in a CSV file
            filename = 'holders.csv'

            if len(values) != 0:
                with open(filename, 'a+', newline='') as file:
                    f = csv.writer(file)
                    for i in range(len(values)):
                        f.writerow([values[i], values1[i]])
    """


# main driver #
if __name__ == "__main__":
    # Create Instance called 'cl' as in "c"raigs "l"ist
    now = datetime.now()  # current date and time
    date_time = now.strftime("%Y-%m-%d_%H_%M")
    file_name = f'results_{date_time}.csv'

    s = get_project_settings()
    s['FEED_FORMAT'] = 'csv'
    s['LOG_LEVEL'] = 'INFO'
    s['FEED_URI'] = file_name
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
