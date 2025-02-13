import scrapy
from scrapy import cmdline
from scrapy.http import HtmlResponse, JsonRequest
import redis
import hashlib
import json

# 1. spider
# 使用start_requests构建初始查询参数：明确爬取的分类
# 有些品牌属于多种类型，对同默认类型(industryType)进行ID去重
# 2. pipelines
# 根据分类建表储存item 

class WinshangdataSpider(scrapy.Spider):

    name = "winshangdata"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis_client = redis.Redis()

    def __del__(self):
        self.redis_client.close()

    def start_requests(self):
        url = "http://www.winshangdata.com/wsapi/brand/getBigdataList3_4"
        types = ["餐饮", "儿童亲子", "文体娱", "零售", "生活服务", "其它类型"]
        for tp in types:
            data = {
                "isHaveLink": "",
                "isTuozhan": "",
                "isXxPp": "",
                "kdfs": "",
                "key": "",
                "orderBy": "1",
                "pageNum": 1,
                "pageSize": 2000,
                "pid": "",
                "qy_p": "",
                "qy_r": "",
                "xqMj": "",
                "ytlb1": tp,
                "ytlb2": ""
            }
            yield JsonRequest(url=url, data=data, dont_filter=False)

    def parse(self, response: HtmlResponse):

        res = response.json()
        data_list = res["data"]["list"]
        for brand in data_list:
            brandId = brand["brandId"]
            industryType = brand["list_IndustryType"]
            
            id_info = {
                "brandId": brandId,
                "industryType": industryType
            } 
            id_info_str = json.dumps(id_info)
            md5_hash = hashlib.md5()
            md5_hash.update(id_info_str.encode())
            hash_value = md5_hash.hexdigest()
            if self.redis_client.get(f'wsBrands_filter: {hash_value}'):
                print('data duplication')
                
            else:
                self.redis_client.set(f'wsBrands_filter: {hash_value}', id_info_str)
                detail_url = f"http://www.winshangdata.com/brandDetail?brandId={id_info['brandId']}"
                yield scrapy.Request(detail_url, callback=self.process_detail, cb_kwargs={
                    'brandId': brandId, "industryType": industryType})

    def process_detail(self, response, brandId, industryType):
      
        title = response.xpath('//div[@class="tit-name"]/div/div/text()').get()
        li_list = response.xpath('//ul[@class="detail-option border-b"]/li')
        
        fieldIndices = {
            "创建时间": 1,
            "开店方式": 3,
            "合作期限": 4,
            "面积要求": 5
        }

        fields = {key: li_list[index].xpath('.//span[2]/text()').get() 
                for key, index in fieldIndices.items()}

        item = {
            "ID": brandId,
            "类型": industryType,
            "标题": title
            }
        item.update(fields)
        yield item


if __name__ == "__main__":
    cmdline.execute("scrapy crawl winshangdata".split())