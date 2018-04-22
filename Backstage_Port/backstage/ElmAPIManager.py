import configparser
import requests
import time
import json
import datetime
from Backstage_Port.backstage.ParseElmbackstage import PaerseElmbackstage
from Backstage_Port.Comment.comment_label import deal_comment
from Backstage_Port.backstage.MeituanAPI import meituan_web_crawl,meituan_daily_and_operation_analysis
from util.utilFunction import print_run_time


class ElmAPIManager():
    def __init__(self):
        self.todaydate = datetime.date.today()
        self.yesterday = self.todaydate-datetime.timedelta(days=1)
        self.tomorrow = self.todaydate+datetime.timedelta(days=1)
        self.before7day = self.todaydate-datetime.timedelta(days=7)

        self.todaytime = datetime.datetime.now().strftime('%Y-%m-%dT00:00:00')
        self.yesterdaytime = self.yesterday.strftime('%Y-%m-%dT00:00:00')
        self.tomorrowtime = self.tomorrow.strftime('%Y-%m-%dT00:00:00')

        # print(self.todaytime,self.yesterdaytime,self.tomorrowtime)
        # print(self.todaydate,self.yesterday,self.tomorrow)

    def getConfig(self,xpath_path):
        """
            获取配置文件中的xpath路径
        :param config:
        :return: 返回字典
        """
        cfg = configparser.ConfigParser()
        cfg.read(xpath_path,encoding='utf-8')

        params = dict()
        for s in cfg.sections():
            keys = cfg.options(s)
            arr = []
            for k in keys:
                xpath = cfg.get(s, k)
                arr.append(xpath)
            params[s] = arr
        return params

    def getShopRatingStats(self,shopid):
        """
        获取顾客下顾客评价的数据，昨天
        :return:
        """
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[2]
        ksid = params.get(shopid)[1]
        print(x_shard)

        format_url = 'https://app-api.shop.ele.me/ugc/invoke?method=shopRating.getShopRatingStats'
        headers = {
                    'accept':'application/json, text/plain, */*',
                    'Accept-Encoding':'gzip, deflate, br',
                    'Accept-Language':'zh-CN,zh;q=0.9',
                    'Connection':'keep-alive',
                    'content-type':'application/json;charset=UTF-8',
                    'Host':'app-api.shop.ele.me',
                    'Origin':'https://melody-stats.faas.ele.me',
                    'Referer':'https://melody-stats.faas.ele.me/',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
                    'x-eleme-requestid':x_eleme_requestid,
                    'x-shard':x_shard
                  }
        data = {"id":x_eleme_requestid,
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "server":"https://app-api.shop.ele.me/",
                         "ksid":ksid},
                "service":"shopRating",
                "method":"getShopRatingStats",
                "params":{"shopId":shopid,
                          "query":{"beginDate":self.yesterdaytime,
                                   "endDate":self.todaytime,
                                   "hasContent":"true",
                                   "level":"null",
                                   "replied":"null",
                                   "tag":"null",
                                   "limit":20,
                                   "offset":0,
                                   "state":"null"}},
                "ncp":"2.0.0"}
        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("昨天的顾客评价：",result)
        return result

    def getTodayBusinessStatistics(self,shopid):
        """
        获取数据下营业统计分析的数据 今天
        :return:
        """
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[3]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/stats/invoke/?method=saleStatsNew.getTodayBusinessStatisticsV3'
        headers = {
                    'accept':'application/json, text/plain, */*',
                    'Accept-Encoding':'gzip, deflate, br',
                    'Accept-Language':'zh-CN,zh;q=0.9',
                    'Connection':'keep-alive',
                    'content-type':'application/json;charset=UTF-8',
                    'Host':'app-api.shop.ele.me',
                    'Origin':'https://melody-stats.faas.ele.me',
                    'Referer':'https://melody-stats.faas.ele.me/',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
                    'x-eleme-requestid':x_eleme_requestid,
                    'x-shard':x_shard
        }
        data = {"id":x_eleme_requestid,
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "server":"https://app-api.shop.ele.me/",
                         "ksid":ksid},
                "service":"saleStatsNew",
                "method":"getTodayBusinessStatisticsV3",
                "params":{"shopId":shopid},
                "ncp":"2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("今天的营业统计：",result)
        return result

    def getYesterdayDefaultTimeStatistics(self,shopid):
        """
        获取数据下营业统计分析的数据 昨天
        :return:
        """
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[4]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/stats/invoke/?method=saleStatsNew.getHistoryBusinessStatisticsV3'
        headers = {
                    'accept':'application/json, text/plain, */*',
                    'Accept-Encoding':'gzip, deflate, br',
                    'Accept-Language':'zh-CN,zh;q=0.9',
                    'Connection':'keep-alive',
                    'content-type':'application/json;charset=UTF-8',
                    'Host':'app-api.shop.ele.me',
                    'Origin':'https://melody-stats.faas.ele.me',
                    'Referer':'https://melody-stats.faas.ele.me/',
                    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
                    'x-eleme-requestid':x_eleme_requestid,
                    'x-shard':x_shard
        }
        data = {"id":x_eleme_requestid,
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "server":"https://app-api.shop.ele.me/",
                         "ksid":ksid},
                "service":"saleStatsNew",
                "method":"getHistoryBusinessStatisticsV3",
                "params":{"shopId":shopid,
                          "startDate":str(self.yesterday),
                          "endDate":str(self.yesterday)},
                "ncp":"2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("昨天的营业统计：",result)
        return result


    def getTrafficStats(self,shopid):
        """
        获取数据下流量分析的数据，昨天,传的日期必须是当前时间的前一天
        :return:
        """
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[5]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/stats/invoke/?method=trafficStats.getTrafficStatsV2'
        headers = {
            'accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "337",
            'content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://melody-stats.faas.ele.me",
            'Referer': "https://melody-stats.faas.ele.me/",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'x-eleme-requestid': x_eleme_requestid,
            'x-shard': x_shard,
        }
        data = {"id":x_eleme_requestid,
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "server":"https://app-api.shop.ele.me/",
                         "ksid":ksid},
                "service":"trafficStats",
                "method":"getTrafficStatsV2",
                "params":{"shopId":shopid,
                          "beginDate":str(self.yesterday),
                          "endDate":str(self.yesterday)},
                "ncp":"2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("昨天的流量分析：",result)
        return result

    def getRestaurantConsumerStats(self,shopid):
        """
        获取数据下顾客分析的数据，昨天,传的时间必须是当天的前一天
        :return:
        """
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[6]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/stats/invoke/?method=consumerStats.getRestaurantConsumerStats'
        headers = {
            'accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "347",
            'content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://melody-stats.faas.ele.me",
            'Referer': "https://melody-stats.faas.ele.me/",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'x-eleme-requestid': x_eleme_requestid,
            'x-shard': x_shard,
        }
        data = {"id":x_eleme_requestid,
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "server":"https://app-api.shop.ele.me/",
                         "ksid":ksid},
                "service":"consumerStats",
                "method":"getRestaurantConsumerStats",
                "params":{"shopId":shopid,
                          "beginDate":str(self.yesterday),
                          "endDate":str(self.yesterday)},
                "ncp":"2.0.0"}
        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("昨天的顾客分析：",result)
        return result

    def getUVSummary(self,shopid):
        '''
        获取店铺推广下竞价推广的数据
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[7]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/alchemy/invoke/?method=wager.getUVSummary'
        headers = {
            'Accept': "*/*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "281",
            'Content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://bs-river-bid.faas.ele.me",
            'Referer': "https://bs-river-bid.faas.ele.me/",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'X-Eleme-RequestID': x_eleme_requestid,
            'X-Shard': x_shard,
        }

        data = {"id":x_eleme_requestid,
                "method":"getUVSummary",
                "service":"wager",
                "params":{"restaurantId":shopid,
                          "beginTime":str(self.before7day),
                          "endTime":str(self.yesterday)},
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "ksid":ksid},
                "ncp":"2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("前7天的竞价分析：", result)
        return result

    def getSkuActivityByShopIdAndStatusForPlatform(self,shopid):
        '''
        获取营销平台活动的信息
        :param shopid:
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[8]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/marketing/invoke/?method=SkuActivityService.getSkuActivityByShopIdAndStatusForPlatform'
        headers = {
            'Accept': "*/*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "325",
            'Content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://melody-activity.faas.ele.me",
            'Referer': "https://melody-activity.faas.ele.me/app/single/activity/plantform/apply",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'X-Eleme-RequestID': x_eleme_requestid,
            'X-Shard': x_shard,
        }
        data = {"id":x_eleme_requestid,
                "method":"getSkuActivityByShopIdAndStatusForPlatform",
                "service":"SkuActivityService",
                "params":{"shopId":shopid,
                          "status":"ACTIVATED",
                          "page":{"pageNo":1,
                                  "pageSize":10}},
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "ksid":ksid},
                "ncp":"2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("当前SKU活动信息：", result)
        return result

    def getActivityByShopIdAndStatus(self,shopid):
        '''
        获取营销下平台活动的信息
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[9]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/marketing/invoke/?method=activityCenter.getActivityByShopIdAndStatus'
        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "281",
            'content-type': "application/json;charset=UTF-8",
            'host': "app-api.shop.ele.me",
            'origin': "https://melody-activity.faas.ele.me",
            'referer': "https://melody-activity.faas.ele.me/app/single/activity/plantform/manage",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-eleme-requestid': x_eleme_requestid,
            'x-shard': x_shard,
        }

        data = {"id":x_eleme_requestid,
               "method": "getActivityByShopIdAndStatus",
               "service": "activityCenter",
               "params": {"shopId": shopid,
                          "activityStatus": "ACTIVATED"},
               "metas": {"appName": "melody",
                         "appVersion": "4.4.0",
                         "ksid": ksid},
               "ncp": "2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("当前活动信息：", result)
        return result

    def getTurnoverSummary(self,shopid):
        '''
        获取店铺推广下推广资金的信息
        :param shopid:店铺id
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[10]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/alchemy/invoke/?method=wager.getTurnoverSummary'
        headers = {
            'Accept': "*/*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "309",
            'Content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://melody-pay.faas.ele.me",
            'Referer': "https://melody-pay.faas.ele.me/",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'X-Eleme-RequestID': x_eleme_requestid,
            'X-Shard': x_shard,
        }
        data = {"id": x_eleme_requestid,
                "method": "getTurnoverSummary",
                "service": "wager",
                "params": {"restaurantId": shopid,
                           "offset": 0,
                           "limit": 10,
                           "beginTime": str(self.yesterday),
                           "endTime": str(self.yesterday)},
                "metas": {"appName": "melody",
                          "appVersion": "4.4.0",
                          "ksid":ksid},
                "ncp": "2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url,headers=headers, data = json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("推广资金：", result)
        return result

    def getzhifubaoOrder(self,shopid):
        '''
        获取店铺推广下支付宝推广的数据
        :param shopid:
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[11]
        ksid = params.get(shopid)[1]


        format_url = 'https://app-api.shop.ele.me/alchemy/invoke/?method=AlipayPullNewStatsService.getOrderDetails'
        headers = {
            'Accept': "*/*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "292",
            'Content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://vas-alipay-flow-pipe.faas.ele.me",
            'Referer': "https://vas-alipay-flow-pipe.faas.ele.me/",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'X-Eleme-RequestID': x_eleme_requestid,
            'X-Shard': x_shard,
        }
        data = {"id":x_eleme_requestid,
                "method":"getOrderDetails",
                "service":"AlipayPullNewStatsService",
                "params":{"shopId":shopid,
                          "offset":0,
                          "limit":10,
                          "date":str(self.yesterday)},
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "ksid":ksid},
                "ncp":"2.0.0"}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers,data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("支付宝推广资金：", result)
        return result

    def CustomerService(self,shopid):
        '''
        顾客营销下面的精准营销，另外创建一张新表
        :param shopid:
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[12]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/crm/invoke/?method=CustomerService.getGroups'

        headers = {
            'accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "234",
            'content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://throne.faas.ele.me",
            'Referer': "https://throne.faas.ele.me/home/customGroup",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'x-shard': x_shard,
        }

        data = {"id":x_eleme_requestid,
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "ksid":ksid},
                "ncp":"2.0.0",
                "service":"CustomerService",
                "method":"getGroups",
                "params":{"shopId":shopid}}

        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("精准营销信息：", result)
        return result

    def getMarketingRecords(self,shopid):
        '''
        顾客评价下营销历史的信息
        :param shopid:
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[13]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/crm/invoke/?method=MarketingService.getMarketingRecords'

        headers = {
            'accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "266",
            'content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://throne.faas.ele.me",
            'Referer': "https://throne.faas.ele.me/home/precisionMarketing/history",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'x-shard': x_shard,
        }

        data = {"id":x_eleme_requestid,
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "ksid":ksid},
                "ncp":"2.0.0",
                "service":"MarketingService",
                "method":"getMarketingRecords",
                "params":{"shopId":shopid,
                          "offset":0,
                          "limit":5}}
        time.sleep(0.5)
        response = requests.post(format_url, headers=headers, data=json.dumps(data))
        result = json.loads(response.content.decode('utf-8'))
        print("营销历史信息：", result)
        return result

    def bill_message(self):
        '''
        预留接口：账单记录
        :return:
        '''
        format_url = 'https://mdc-httpizza.ele.me/hydros/bill/list?restaurantId=160279990&beginDate=1514736000000&endDate=1515340799999&status=3&offset=0&limit=10&loginRestaurantId=160279990&authType=eleme'
        headers = {
            'Accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Type': "application/json;charset=utf-8",
            'Cookie': "perf_ssid=hmq291qgxqz6n0f9zfjyfcbrrm4n7bpf_2018-01-04; ubt_ssid=ikz509plyutqpzjgy1u5c7ir1hw7zqi8_2018-01-04; _utrace=d635610bbed48a608f23f96873b73efa_2018-01-04; _ga=GA1.2.119381843.1515133113; _gid=GA1.2.1709976364.1515295020; FE_NAPOS_OPEN_AUTH=occIoFk%2B5A2qZsqXOntDpagRFpDTIWfReYG6nYK9YxA%3D",
            'Host': "mdc-httpizza.ele.me",
            'Origin': "https://hydrosweb.faas.ele.me",
            'Referer': "https://hydrosweb.faas.ele.me/bill/0?shopId=160279990",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'X-Shard': "shopId=160279990"
        }

    def getFoodSalesStatsV2(self):
        '''
        预留接口：数据下的商品分析
        :return:
        '''
        format_url = 'https://app-api.shop.ele.me/stats/invoke/?method=foodSalesStats.getFoodSalesStatsV2'
        headers = {
            'accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "417",
            'content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://melody-stats.faas.ele.me",
            'Referer': "https://melody-stats.faas.ele.me/",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'x-eleme-requestid': "F4881B982BDB41A1BA75C4C187FA261C|1515313993038",
            'x-shard': "shopid=160279990"
        }
        data = {"id":"F4881B982BDB41A1BA75C4C187FA261C|1515313993038","metas":{"appName":"melody","appVersion":"4.4.0","server":"https://app-api.shop.ele.me/","ksid":"YjI4YTliNDktMDYxMC00YzdkLWFiMTMTFkYm"},"service":"foodSalesStats","method":"getFoodSalesStatsV2","params":{"shopId":160279990,"foodSalesQuery":{"asc":false,"beginDate":"2018-01-06","endDate":"2018-01-06","limit":10,"orderBy":"SALES_AMOUNT","page":1}},"ncp":"2.0.0"}

    def queryByStatus(self,shopid):
        '''
        预留接口：自建活动数据1
        :return:
        '''
        params = self.getConfig('elm_config.ini')  # 获取店铺配置信息
        x_shard = params.get(shopid)[0]
        x_eleme_requestid = params.get(shopid)[13]
        ksid = params.get(shopid)[1]

        format_url = 'https://app-api.shop.ele.me/coupon/invoke/?method=ActivityService.queryByStatus'
        headers = {
            'Accept': "*/*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "275",
            'Content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://melody-activity.faas.ele.me",
            'Referer': "https://melody-activity.faas.ele.me/app/single/activity/self/create",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'X-Eleme-RequestID': x_eleme_requestid,
            'X-Shard': x_shard
        }
        data = {"id":x_eleme_requestid,
                "method":"queryByStatus",
                "service":"ActivityService",
                "params":{"shopId":shopid,
                          "activityStatusSpecific":"ACTIVATED"},
                "metas":{"appName":"melody",
                         "appVersion":"4.4.0",
                         "ksid":ksid},
                "ncp":"2.0.0"}

        # time.sleep(0.5)
        # response = requests.post(format_url, headers=headers, data=json.dumps(data))
        # result = json.loads(response.content.decode('utf-8'))
        # print("自建活动信息1：", result)
        # return result

    def getActivityStatus(self):
        '''
        获取自建活动的数据
        :return:
        '''
        format_url  = 'https://app-api.shop.ele.me/marketing/invoke/?method=activityCenter.getActivityByShopIdAndStatus'
        headers = {
            'Accept': "*/*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Content-Length': "281",
            'Content-type': "application/json;charset=UTF-8",
            'Host': "app-api.shop.ele.me",
            'Origin': "https://melody-activity.faas.ele.me",
            'Referer': "https://melody-activity.faas.ele.me/app/single/activity/self/create",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
            'X-Eleme-RequestID': "043fd63a-a3e0-4b62-acc9-0ddc458cb10d",
            'X-Shard': "shopid=160279990"
        }
        data = {"id":"043fd63a-a3e0-4b62-acc9-0ddc458cb10d","method":"getActivityByShopIdAndStatus","service":"activityCenter","params":{"shopId":160279990,"activityStatus":"ACTIVATED"},"metas":{"appName":"melody","appVersion":"4.4.0","ksid":"YjI4YTliNDktMDYxMC00YzdkLWFiMTMTFkYm"},"ncp":"2.0.0"}





def run():
    '''
    根据店铺id信息获取商家后台信息
    :return:
    '''
    shopid = ['160279990', '161289358', '161378073', '161313783','161341608','161506911','161507150']
    #暂时义乌店关店，后面的重新开店需要重新写
    # shopid = ['161289358', '161378073', '161313783', '161341608', '161506911', '161507150']

    for item in shopid:
        print("开始爬取店铺%s商家后台数据>>>>>" % item)
        elm_api = ElmAPIManager()
        ShopRatingStats = elm_api.getShopRatingStats(item)
        TodayBusinessStatistics = elm_api.getTodayBusinessStatistics(item)
        getYesterdayDefaultTimeStatistics = elm_api.getYesterdayDefaultTimeStatistics(item)
        TrafficStats = elm_api.getTrafficStats(item)
        RestaurantConsumerStats = elm_api.getRestaurantConsumerStats(item)
        getUVSummary = elm_api.getUVSummary(item)
        getSkuActivityStatus = elm_api.getSkuActivityByShopIdAndStatusForPlatform(item)
        ActivityByShopIdAndStatus = elm_api.getActivityByShopIdAndStatus(item)
        getTurnoverSummary = elm_api.getTurnoverSummary(item)
        getzhifubaoOrder = elm_api.getzhifubaoOrder(item)
        customerservice = elm_api.CustomerService(item)
        getMarketingRecords = elm_api.getMarketingRecords(item)
        # queryByStatus = elm_api.queryByStatus(item)


        print("店铺%s商家后台数据开始入库>>>>>" % item)
        parse_elm_data = PaerseElmbackstage(item, ShopRatingStats, getYesterdayDefaultTimeStatistics,
                                            RestaurantConsumerStats,
                                            getTurnoverSummary, getUVSummary, getzhifubaoOrder,
                                            customerservice, getMarketingRecords, TodayBusinessStatistics,
                                            TrafficStats, getSkuActivityStatus, ActivityByShopIdAndStatus)
        yield parse_elm_data

@print_run_time
def elm_web_crawl():
    '''
    网页端爬取商家后台的数据
    :param parse_elm_data:
    :return:
    '''
    parse_elm_data = run()
    for item in parse_elm_data:
        item.deal_ShopRatingStats()
        item.deal_TrafficStats()
        item.deal_ActivityStatus()
        item.deal_getSkuActivityStatus()

@print_run_time
def elm_operation_crawl():
    '''
    运营端爬取商家后台的数据
    :param parse_elm_data:
    :return:
    '''
    parse_elm_data = run()
    for item in parse_elm_data:
        item.deal_Daily_Operational_Data()
        item.deal_Operational_data_analysis()
        item.deal_CustomerService()
        item.deal_getMarketingRecords()

@print_run_time
def deal_comment_to_DB():
    '''
    处理商家后台评论信息，将差评进行分类
    :return:
    '''
    print("开始处理商家后台差评信息>>>>>")
    deal_comment()


def deal_WMDS_elm_data():
    '''
    处理外卖大师的数据
    :return:
    '''
    parse_elm_data = run()
    WMDS_elm_data_all = []
    for item in parse_elm_data:
        WMDS_elm_data = item.deal_WMDS_elm_data()
        WMDS_elm_data_all.append(WMDS_elm_data)
    return WMDS_elm_data_all


if __name__ == "__main__":
    pass



