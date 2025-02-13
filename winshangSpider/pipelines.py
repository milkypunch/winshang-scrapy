# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo

class WinshangspiderPipeline:

    def open_spider(self, spider):
        self.mongo_client = pymongo.MongoClient()
        
        
        self.collections = {
            "餐饮": self.mongo_client['winshang_brands']['餐饮'],
            "儿童亲子": self.mongo_client['winshang_brands']['儿童亲子'],
            "文体娱": self.mongo_client['winshang_brands']['文体娱'],
            "零售": self.mongo_client['winshang_brands']['零售'],
            "生活服务": self.mongo_client['winshang_brands']['生活服务'],
            "其它类型": self.mongo_client['winshang_brands']['其它类型']
        }
        
    def process_item(self, item, spider):
        type_ = item.get("类型")
        collection = self.collections.get(type_)  
        
        if collection is not None:  
            collection.insert_one(item)  

    def close_spider(self, spider):
        self.mongo_client.close()

