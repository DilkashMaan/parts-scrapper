# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd

class ExcelPipeline:
    def process_item(self, item, spider):
        spider.alldata.append(item)
        if len(spider.alldata) >= spider.save_interval:
            spider.save_progress()
        return item




class LinksPipeline:
    def process_item(self, item, spider):
        return item
