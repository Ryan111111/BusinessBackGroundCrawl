import requests
import json
import configparser
from Backstage_Port.backstage.ParseMeituanbackstage import PaerseMeituanbackstage
import time
import datetime
from util.utilFunction import print_run_time

class MeituanAPI():

    def __init__(self,shopid):
        self.wmPoiId = self.get_wmPoiId(shopid)
        self.yesterday = datetime.date.today() - datetime.timedelta(days=1)

    def get_wmPoiId(self,shopid):
        if shopid == 'wmxhxq1392':
            wmPoiId = 3613406
            return wmPoiId
        if shopid == 'wmxhxq168673':
            wmPoiId = 4070324
            return wmPoiId
        if shopid == 'wmxhxq1791':
            wmPoiId = 4104131
            return wmPoiId
        if shopid == 'wmxhxq18423':
            wmPoiId = 3685153
            return wmPoiId
        if shopid == 'wmxhxq19610':
            wmPoiId = 4166456
            return wmPoiId
        if shopid == 'wmxhxq20715':
            wmPoiId = 4210216
            return wmPoiId



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

    def commentSummary(self,shopid):
        '''
        获取售后管理的用户评价的数据
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[0]
        cookie = params.get(shopid)[1]
        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.8",
            'connection': "keep-alive",
            'cookie':cookie,
            'host': "e.waimai.meituan.com",
            'referer': "http://e.waimai.meituan.com/v2/customer/comment",
            'user-agent': "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("用户评价信息：",result)
        return result

    def businessStatistics(self,shopid):
        '''
        经营分析下营业统计的数据：营业额，有效订单，订单收入，活动补贴，无效订单
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[2]
        cookie = params.get(shopid)[3]
        headers = {
            'accept': "application/json, text/plain, */*",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'cookie':cookie,
            'host': "waimaieapp.meituan.com",
            'referer': "https://waimaieapp.meituan.com/bizdata/?_source=PC&token=0CGqBSWjy2O429iT7Ba_n7nF3na1FmUufMpcRPl7lrGI*&acctId=30714514&wmPoiId=3613406",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("营业统计信息：",result)
        return result

    def getTrafficStats(self,shopid):
        '''
        获取经营分析下流量分析的数据：曝光人数，访问人数，下单人数，访问转化率，下单转化率
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[4]
        cookie = params.get(shopid)[5]
        headers = {
            'accept': "application/json, text/plain, */*",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'cookie':cookie,
            'host': "waimaieapp.meituan.com",
            'referer': "https://waimaieapp.meituan.com/bizdata/?_source=PC&token=0CGqBSWjy2O429iT7Ba_n7nF3na1FmUufMpcRPl7lrGI*&acctId=30714514&wmPoiId=3613406",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("流量分析信息：",result)
        return result

    def customerAnalysis(self,shopid):
        '''
        获取经营分析下顾客分析的数据：下单人数，新客人数，老客人数
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[6]
        cookie = params.get(shopid)[7]
        headers = {
            'accept': "application/json, text/plain, */*",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'cookie':cookie,
            'host': "waimaieapp.meituan.com",
            'referer': "https://waimaieapp.meituan.com/bizdata/?_source=PC&token=0CGqBSWjy2O429iT7Ba_n7nF3na1FmUufMpcRPl7lrGI*&acctId=30714514&wmPoiId=3613406",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("顾客分析信息：",result)
        return result

    def customerPerfer(self,shopid):
        '''
        获取经营分析下顾客喜好的数据：本店顾客爱好，本店顾客喜欢的活动
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[8]
        cookie = params.get(shopid)[9]
        headers = {
            'accept': "application/json, text/plain, */*",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'cookie': cookie,
            'host': "waimaieapp.meituan.com",
            'referer': "https://waimaieapp.meituan.com/bizdata/?_source=PC&token=0CGqBSWjy2O429iT7Ba_n7nF3na1FmUufMpcRPl7lrGI*&acctId=30714514&wmPoiId=3613406",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("顾客爱好信息：",result)
        return result

    def effectiveOrders(self,shopid):
        '''
        获取经营分析下营业分析昨日有效订单的数据：营业额、有效订单、单均价、订单收入、活动补贴
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[10]
        get_date_url = ''.join(str(self.yesterday).split('-'))
        date_url = '&beginTime='+get_date_url+'&endTime='+get_date_url
        url = url+date_url
        print(url)

        cookie = params.get(shopid)[11]
        headers = {
            'accept': "application/json, text/plain, */*",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'cookie': cookie,
            'host': "waimaieapp.meituan.com",
            'referer': "https://waimaieapp.meituan.com/bizdata/?_source=PC&token=0CGqBSWjy2O429iT7Ba_n7nF3na1FmUufMpcRPl7lrGI*&acctId=30714514&wmPoiId=3613406",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("营业同行信息：",result)
        return result

    def todaystatistics(self,shopid):
        '''
        获取今日的门店推广的基本信息：曝光提升次数、访问提升次数、推广消费、单次访问消费
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[12]
        cookie = params.get(shopid)[13]

        headers = {
            'accept': "application/json, text/javascript, */*; q=0.01",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'cookie': cookie,
            'host': "waimaieapp.meituan.com",
            'referer': "https://waimaieapp.meituan.com/ad/v1/pc?_source=PC&token=0CGqBSWjy2O429iT7Ba_n7nF3na1FmUufMpcRPl7lrGI*&acctId=30714514&wmPoiId=3613406",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("门店推广信息：",result)
        return result

    def Activitylist(self,shopid):
        '''
        获取店铺活动信息：活动类型、活动名称、活动内容
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[14]
        cookie = params.get(shopid)[15]

        headers = {
            'accept': "*/*",
            'accept-encoding': "gzip, deflate, br",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'content-length': "15",
            'content-type': "application/x-www-form-urlencoded; charset=UTF-8",
            'cookie':cookie,
            'host': "waimaieapp.meituan.com",
            'origin': "https://waimaieapp.meituan.com",
            'referer': "https://waimaieapp.meituan.com/reuse/activity/setting/r/getPcIndexPage?_source=PC&token=0Y9dnzILEAMxX3qnckWtlHiizNlxuVorGJ8UU5RUSkbs*&acctId=30714514&wmPoiId=3613406",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest",
        }
        data = { "wmPoiId":self.wmPoiId}
        time.sleep(0.5)
        response = requests.post(url, headers=headers,data=data)
        result = json.loads(response.content.decode('utf-8'))
        print("店铺活动信息：", result)
        return result


    def CustomerReminderInfo(self,shopid):
        '''
        获取店铺催单的账号ID信息
        :param shopid:
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[16]
        cookie = params.get(shopid)[17]

        headers = {
                      'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
                      'accept': "*/*",
                      'accept-encoding': "gzip, deflate",
                      'accept-language': "zh-CN,zh;q=0.9",
                      'connection': "keep-alive",
                      'cookie':cookie,
                      'host': "e.waimai.meituan.com",
                      'referer': "http://e.waimai.meituan.com/v2/order/reminder",
                      'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
                      'x-requested-with': "XMLHttpRequest"
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("店铺催单账号信息：", result)
        return result

    def CustomerReminder(self,shopid):
        '''
        获取店铺的催单信息
        :param shopid:
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[18]
        cookie = params.get(shopid)[19]

        headers = {
            'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
            'accept': "*/*",
            'accept-encoding': "gzip, deflate",
            'accept-language': "zh-CN,zh;q=0.9",
            'connection': "keep-alive",
            'cookie': cookie,
            'host': "e.waimai.meituan.com",
            'referer': "http://e.waimai.meituan.com/v2/order/reminder",
            'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
            'x-requested-with': "XMLHttpRequest"
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("店铺催单信息：", result)
        return result

    def PeersCompareAnalysis(self,shopid):
        '''
        获取营业统计下昨天对比同行评价数据
        :param shopid:
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[20]
        cookie = params.get(shopid)[21]

        headers = {
            'Accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Cookie': cookie,
            'Host': "waimaieapp.meituan.com",
            'Referer': "https://waimaieapp.meituan.com/bizdata/?_source=PC&token=0CFk2xdFXYP5HhsSMG9WQTet4rRb8bg5UF9OB6pcaVew*&acctId=30714514&wmPoiId=3613406",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
            'X-Requested-With': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("同行对比信息：", result)
        return result

    def getCouponLabel(self,shopid):
        '''
        获取营销下的精准营销的信息：
        :param shopid:
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[22]
        cookie = params.get(shopid)[23]

        headers = {
            'Accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Cookie': cookie,
            'Host': "waimaieapp.meituan.com",
            'Referer': "https://waimaieapp.meituan.com/bizdata/",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
            'X-Requested-With': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("精准营销的信息：", result)
        return result

    def history_consume(self,shopid):
        '''
        获取消费的历史记录
        :param shopid:
        :return:
        '''
        params = self.getConfig('meituan_config.ini')  # 获取店铺配置信息
        url = params.get(shopid)[24]
        cookie = params.get(shopid)[25]

        headers = {
            'Accept': "application/json, text/javascript, */*; q=0.01",
            'Accept-Encoding': "gzip, deflate, br",
            'Accept-Language': "zh-CN,zh;q=0.9",
            'Connection': "keep-alive",
            'Cookie': cookie,
            'Host': "waimaieapp.meituan.com",
            'Referer': "https://waimaieapp.meituan.com/ad/v1/pc?_source=PC&token=0CFk2xdFXYP5HhsSMG9WQTet4rRb8bg5UF9OB6pcaVew*&acctId=30714514&wmPoiId=3613406",
            'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
            'X-Requested-With': "XMLHttpRequest",
        }
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        result = json.loads(response.content.decode('utf-8'))
        print("消费记录的信息：", result)
        return result


def deal_WMDS_meituan_data():
    '''
    处理美团外卖大师的数据
    :return:
    '''
    parsedata = meituan_run()
    WMDS_meituan_data_all_info = []
    for item in parsedata:
        WMDS_meituan_data = item.WMDS_meituan_data()
        WMDS_meituan_data_all_info.append(WMDS_meituan_data)
    return WMDS_meituan_data_all_info


@print_run_time
def meituan_web_crawl():
    '''
    处理web端商家后台的数据
    :param parsedata:
    :return:
    '''
    parsedata = meituan_run()
    for item in parsedata:
        item.deal_commentSummary()
        item.deal_TrafficStats()
        item.deal_Activitylist()
        item.deal_CustomerReminder()

@print_run_time
def meituan_daily_and_operation_analysis():
    '''
    处理运营端数据分析，和商家后台数据
    :param parsedata:
    :return:
    '''
    parsedata = meituan_run()
    for item in parsedata:
        item.deal_meituan_customer_prefer_analysis()
        item.deal_meituan_customer_prefer_activity()
        item.deal_meituan_customer_precision_operation()
        item.deal_meituan_Operational_data_analysis()

        item.deal_meituan_daily_operational_data()





def meituan_run():
    '''
    主程序，爬取店铺商家后台的信息，并且将数据入库
    :return:
    '''
    shop_id = ['wmxhxq1392','wmxhxq168673','wmxhxq1791','wmxhxq19610','wmxhxq18423','wmxhxq20715']
    for item in shop_id:
        print("开始爬去店铺的美团商家后台的数据>>>>>")
        meituanapi = MeituanAPI(item)
        commentSummary = meituanapi.commentSummary(item)
        businessStatistics = meituanapi.businessStatistics(item)
        TrafficStats = meituanapi.getTrafficStats(item)
        customerAnalysis = meituanapi.customerAnalysis(item)
        customerPerfer = meituanapi.customerPerfer(item)
        effectiveOrders = meituanapi.effectiveOrders(item)
        todaystatistics = meituanapi.todaystatistics(item)
        activitylist = meituanapi.Activitylist(item)
        customer_reminderInfo = meituanapi.CustomerReminderInfo(item)
        customer_reminder = meituanapi.CustomerReminder(item)
        PeersCompareAnalysis = meituanapi.PeersCompareAnalysis(item)
        getCouponLabel = meituanapi.getCouponLabel(item)
        history_consume = meituanapi.history_consume(item)


        print("商家后台的数据开始入库>>>>>")
        parsedata = PaerseMeituanbackstage(commentSummary, businessStatistics, TrafficStats, customerAnalysis,
                                           customerPerfer, effectiveOrders, todaystatistics, activitylist,
                                           customer_reminderInfo, customer_reminder,history_consume,
                                           PeersCompareAnalysis,getCouponLabel,item)

        yield parsedata





if __name__ == '__main__':
    pass




