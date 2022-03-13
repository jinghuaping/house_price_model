# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter
from datetime import datetime
import os


class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['listing_id'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['listing_id'])
            return item


class HousePriceScraperPipeline:
    def __init__(self):
        try:
            now = datetime.now()  # current date and time
            date_time = now.strftime("%Y-%m-%d_%H_%M")
            self.filename = f'results_{date_time}.csv'
            os.remove(self.filename)
        except OSError:
            pass

    def open_spider(self, spider):
        self.csvfile = open(self.filename, mode='wb')
        self.exporter = CsvItemExporter(self.csvfile)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.csvfile.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
