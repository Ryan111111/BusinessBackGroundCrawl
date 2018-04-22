import datetime
from util.DB.DAO import DBUtils,BatchSql


class PaerseElmbackstage():
   '''
   解析商家后台数据，并写入相应的数据库
   '''
   def __init__(self,shopid,ShopRatingStats,getYesterdayDefaultTimeStatistics,RestaurantConsumerStats,
                getTurnoverSummary,getUVSummary,getzhifubaoOrder,
                customerservice,getMarketingRecords,TodayBusinessStatistics,
                TrafficStats,getSkuActivityStatus,ActivityByShopIdAndStatus,
                ):

        self.shopid = shopid
        self.ShopRatingStats = ShopRatingStats
        self.getYesterdayDefaultTimeStatistics = getYesterdayDefaultTimeStatistics
        self.RestaurantConsumerStats = RestaurantConsumerStats
        self.getTurnoverSummary = getTurnoverSummary
        self.getUVSummary = getUVSummary
        self.getzhifubaoOrder = getzhifubaoOrder
        self.customerservice = customerservice
        self.getMarketingRecords = getMarketingRecords
        self.TodayBusinessStatistics = TodayBusinessStatistics
        self.TrafficStats = TrafficStats
        self.getSkuActivityStatus = getSkuActivityStatus
        self.ActivityByShopIdAndStatus = ActivityByShopIdAndStatus
        # self.queryByStatus = queryByStatus
        self.db1 = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'big_data', 'utf8mb4'))
        self.db2 = DBUtils(('192.168.1.200', 3306, 'njjs_zsz', 'njjs1234', 'zszdata', 'utf8mb4'))
        self.db3 = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'wmds', 'utf8mb4'))
        self.date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.yesterday = datetime.date.today() - datetime.timedelta(days=1)
        self.platform = 1  #平台标签
        self.shopname = self.get_shopname(self.shopid)
        self.WMDS_shopid = self.get_WMDS_shopid()

   def get_WMDS_shopid(self):

       if self.shopid == '160279990' or self.shopid == 'wmxhxq1392':
           WMDS_shopid = 10
           return WMDS_shopid
       if self.shopid == '161289358' or self.shopid == 'wmxhxq168673':
           WMDS_shopid = 12
           return WMDS_shopid
       if self.shopid == '161378073':
           WMDS_shopid = 13
           return WMDS_shopid
       if self.shopid == '161313783':
           WMDS_shopid = 14
           return WMDS_shopid
       if self.shopid == '161341608' or self.shopid == 'wmxhxq1791':
           WMDS_shopid = 15
           return WMDS_shopid
       if self.shopid == '156753255':
           WMDS_shopid = 16
           return WMDS_shopid
       if self.shopid == 'wmxhxq18423':
           WMDS_shopid = 18
           return WMDS_shopid
       if self.shopid == 'wmxhxq19610':
           WMDS_shopid = 17
           return WMDS_shopid
       if self.shopid == '161506911':
           WMDS_shopid = 19
           return WMDS_shopid

   def get_shopname(self,shopid):
       if shopid == '160279990':
           shopname = '谢恒兴奇味鸡煲(义乌店)'
           return shopname
       if shopid == '161289358':
           shopname = '谢恒兴奇味鸡煲(河西万达店)'
           return shopname
       if shopid == '161378073':
           shopname = '谢恒兴奇味鸡煲(同曦鸣城店)'
           return shopname
       if shopid == '161313783':
           shopname = '谢恒兴奇味鸡煲(殷巷店)'
           return shopname
       if shopid == '161341608':
           shopname = '谢恒兴奇味鸡煲(明发广场店)'
           return shopname
       if shopid == '156753255':
           shopname = '谢恒兴奇味鸡煲(小市店)'
           return shopname
       if shopid == '161506911':
           shopname = '谢恒兴奇味鸡煲(四方新村店)'
           return shopname
       if shopid == '161507150':
           shopname = '谢恒兴奇味鸡煲(元通店)'
           return shopname


   def deal_WMDS_elm_data(self):
       '''
        处理外卖大师的数据
       :return:
       '''
       try:
           WMDS_elm_data = []
           e_serviceRating = self.ShopRatingStats['result']['serviceRating']
           e_exposureTotalCount = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['exposureTotalCount']
           e_visitorNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorNum']
           e_buyerNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerNum']
           e_visitorConversionRate = (self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorConversionRate']) * 100
           e_buyerConversionRate = (self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerConversionRate']) * 100

           WMDS_elm_data.extend([self.WMDS_shopid,e_serviceRating,e_exposureTotalCount,e_visitorNum,e_buyerNum,e_visitorConversionRate,e_buyerConversionRate])
           print('饿了么外卖大师的数据：',WMDS_elm_data)
           return WMDS_elm_data
       except Exception as e:
           print(e)

   def deal_ShopRatingStats(self):
        '''
        处理顾客评价的信息：店铺ID、时间、平台、评分、评分日比
        :return:
        '''
        ShopRatingStats_content = []
        shopId = self.ShopRatingStats['result']['shopId']
        serviceRating = self.ShopRatingStats['result']['serviceRating']
        zoneRank = self.ShopRatingStats['result']['zoneRank']/100
        ShopRatingStats_content.extend([str(shopId),self.date,self.platform,serviceRating])
        print('店铺评分信息：',ShopRatingStats_content)

        sql = "insert into rating_score VALUES "
        batch = BatchSql(sql)
        batch.addBatch(ShopRatingStats_content)
        self.db1.update(batch)

   def deal_TrafficStats(self):
        '''
        处理顾客数据下流量分析的数据：店铺id、时间、门店曝光人数、进店人数、下单人数、进店转化率、下单转化率
        :return:
        '''
        TrafficStats = []
        shopid = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['shopId']
        exposureTotalCount = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['exposureTotalCount']
        visitorNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorNum']
        buyerNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerNum']
        visitorConversionRate = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorConversionRate']*100
        buyerConversionRate = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerConversionRate']*100
        TrafficStats.extend([0,shopid,self.date,exposureTotalCount,visitorNum,buyerNum,visitorConversionRate,buyerConversionRate])
        print('店铺流量数据：',TrafficStats)

        sql = "insert into elm_backstage VALUES "
        batch = BatchSql(sql)
        batch.addBatch(TrafficStats)
        self.db1.update(batch)

   def deal_getSkuActivityStatus(self):

       SkuStatus = self.getSkuActivityStatus['result']
       for item in SkuStatus:
           SkuActivityStatus = []
           activityId = item['activityId']
           SkuStatus_type = item['icon']['text']
           applyName = item['activityApply']['applyName']
           activity_rules = item['activityContent']['benefit']['content']
           SkuActivityStatus.extend([0,self.shopid,activityId,self.date,1,SkuStatus_type,applyName,activity_rules])
           print('SKU的活动信息：', SkuActivityStatus)

           sql = "insert into daily_shop_activity_data VALUES"
           batch = BatchSql(sql)
           batch.addBatch(SkuActivityStatus)
           self.db3.update(batch)


   def deal_ActivityStatus(self):
        '''
        处理当天的活动信息数据：序号id、店铺id、活动id、时间、平台、活动类型、活动名称、活动规则
        :return:
        '''
        ActivityStatus = []
        Activity = self.ActivityByShopIdAndStatus['result']
        for item in Activity:
            if item['childrenActivityId'] != None:
                activity_id = item['id']
                activity_type = item['iconText']
                activity_name = item['title']
                activity_rules = item['content']['items'][0]['onlinePaymentDiscount']
                ActivityStatus.extend([0,self.shopid,activity_id,self.date,1,activity_type,activity_name,activity_rules])
                print('店铺的活动信息：',ActivityStatus)

                sql = "insert into daily_shop_activity_data VALUES"
                batch = BatchSql(sql)
                batch.addBatch(ActivityStatus)
                self.db3.update(batch)

   # def deal_QueryByStatus_Activity(self):
   #     '''
   #     处理自建活动中的数据：序号id、店铺id、活动id、时间、平台、活动类型、活动名称、活动规则
   #     :return:
   #     '''
   #     QueryByStatus_Activity = self.queryByStatus['result']
   #     for item in QueryByStatus_Activity:
   #         QueryByStatus_Activity = []
   #         metaType = item['metaType']
   #         QueryByStatus_Activity_Name = item['name']
   #         threshold = str(item['condition']['threshold'])
   #         reduction = str(item['benefit']['content']['reduction'])
   #         QueryByStatus_Activity_rules = '固定金额'+reduction+'元满'+threshold+'元可用'
   #
   #         QueryByStatus_Activity.extend([0,self.shopid,self.shopname,str(self.yesterday),
   #                                        metaType,QueryByStatus_Activity_Name,QueryByStatus_Activity_rules])
   #         print('自建活动信息：',QueryByStatus_Activity)
   #
   #         sql = "insert into daily_activity_data VALUES"
   #         # batch = BatchSql(sql)
   #         # batch.addBatch(QueryByStatus_Activity)
   #         # self.db1.update(batch)

   def deal_Daily_Operational_Data(self):
       '''
       运营日报的数据：日期，店铺id，店铺名称，
                    日有效单量，预计净收入,
                    曝光量,进店量,下单量,进店转化率,下单转化率
                    新客人数,新客占比,老客人数,老客占比,
                    推广金额,竞价推广点击次数,单次点击费用
                    支付宝推荐次数

       :param shopid:
       :return: validOrderCount,预计净收入
       '''
       daily_data = []
       try:
           if len(self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List']):
               validOrderCount = self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List'][0]['onlineOrderCount']
               restaurantIncome = self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List'][0]['restaurantIncome']
           else:
               validOrderCount = 0
               restaurantIncome = 0
           exposureTotalCount = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['exposureTotalCount']
           visitorNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorNum']
           buyerNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerNum']
           visitorConversionRate = (self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorConversionRate'])*100
           buyerConversionRate = (self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerConversionRate'])*100


           try:
               newConsumerCnt = self.RestaurantConsumerStats['result'][0]['newConsumerCnt']
               totalConsumerCnt = self.RestaurantConsumerStats['result'][0]['totalConsumerCnt']
               oldConsumerCnt = totalConsumerCnt-newConsumerCnt
               newConsumerPercent = newConsumerCnt/totalConsumerCnt
               oldConsumerPercent = oldConsumerCnt/totalConsumerCnt
           except Exception:
               newConsumerCnt = oldConsumerCnt = newConsumerPercent = oldConsumerPercent = 0

           all_cost_list = self.getTurnoverSummary['result']['details']  #获取当前日的推广总资金
           all_cost = 0
           for item in all_cost_list:
               cost_date = str(item['beginTime']).split('T')[0]
               cost_type = item['costType']
               if cost_date == str(self.yesterday) and cost_type == "WAGER":    #判断是否是消费金额
                       all_cost += item['cost']

           all_Count_list = self.getUVSummary['result']['details']       #获取点击次数
           totalclickCount = 0
           totalclick_mean = 0
           for item in all_Count_list:
               count_date = str(item['timeSign'])
               if count_date == str(self.yesterday):
                   totalclickCount = item['totalCount']
                   totalclickCost = item['totalCost']
                   if totalclickCount == 0:
                       totalclick_mean = 0
                   else:
                       totalclick_mean = totalclickCost/totalclickCount

           AlipayPull = self.getzhifubaoOrder['result']['count']


           shopname = self.get_shopname(self.shopid)
           daily_data.extend([str(self.yesterday),self.shopid,shopname,
                             validOrderCount,restaurantIncome,
                             exposureTotalCount,visitorNum,buyerNum,visitorConversionRate,buyerConversionRate,
                             newConsumerCnt,newConsumerPercent,oldConsumerCnt,oldConsumerPercent,
                             all_cost,totalclickCount,totalclick_mean,
                             AlipayPull])
           print("运营日报的数据:",daily_data)

           sql = "insert into elm_daily_operational_data VALUES"
           batch = BatchSql(sql)
           batch.addBatch(daily_data)
           self.db2.update(batch)
       except Exception as e:
           print('商家后台数据有问题：',e)

   def deal_Operational_data_analysis(self):
       '''
       处理商家后台运营数据分析数据，日期，店铺id，店铺名称
                                有效订单数，营业额，支出，预计净收入
                                综合评分，高于商圈商家百分比
                                门店曝光人数、进店人数、下单人数、进店转化率、下单转化率
                                所有顾客数，新客人数,老顾客数，复购率
                                推广点击次数，推广点击金额
       :return:
       '''
       Operational_data_analysis = []
       try:
           if self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List']:
               validOrderCount = self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List'][0]['onlineOrderCount']
               totalOrderAmount = self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List'][0]['totalOrderAmount']
               totalPayout = self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List'][0]['totalPayout']
               restaurantIncome = self.getYesterdayDefaultTimeStatistics['result']['restaurantSaleDetailV3List'][0]['restaurantIncome']

           else:
               validOrderCount = 0
               totalOrderAmount = 0
               totalPayout = 0
               restaurantIncome = 0

           serviceRating = self.ShopRatingStats['result']['serviceRating']
           increasedScore = self.ShopRatingStats['result']['increasedScore']

           exposureTotalCount = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['exposureTotalCount']
           visitorNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorNum']
           buyerNum = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerNum']
           visitorConversionRate = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['visitorConversionRate'] * 100
           buyerConversionRate = self.TrafficStats['result']['restaurantTrafficStatsList'][0]['buyerConversionRate'] * 100

           try:
               newConsumerCnt = self.RestaurantConsumerStats['result'][0]['newConsumerCnt']
               totalConsumerCnt = self.RestaurantConsumerStats['result'][0]['totalConsumerCnt']
               oldConsumerCnt = totalConsumerCnt-newConsumerCnt
               repurchaseRate = self.RestaurantConsumerStats['result'][0]['repurchaseRate']
           except Exception:
               newConsumerCnt = totalConsumerCnt = oldConsumerCnt = repurchaseRate = 0

           all_Count_list = self.getUVSummary['result']['details']  # 获取点击次数
           totalclickCount = 0
           for item in all_Count_list:
               count_date = str(item['timeSign'])
               if count_date == str(self.yesterday):
                   totalclickCount = item['totalCount']

           all_cost_list = self.getTurnoverSummary['result']['details']  # 获取当前日的推广总资金
           all_cost = 0
           for item in all_cost_list:
               cost_date = str(item['beginTime']).split('T')[0]
               if cost_date == str(self.yesterday):
                   all_cost += item['cost']

           Operational_data_analysis.extend([str(self.yesterday),self.shopid,self.shopname,
                                             validOrderCount,totalOrderAmount,totalPayout,restaurantIncome,
                                             serviceRating,increasedScore,
                                             exposureTotalCount,visitorNum,buyerNum,visitorConversionRate,buyerConversionRate,
                                             totalConsumerCnt,newConsumerCnt,oldConsumerCnt,repurchaseRate,
                                             totalclickCount,all_cost])
           print("商家后台运营数据分析:",Operational_data_analysis)

           sql = "insert into elm_operational_data_analysis VALUES"
           batch = BatchSql(sql)
           batch.addBatch(Operational_data_analysis)
           self.db2.update(batch)
       except Exception as e:
           print('商家后台数据有问题：',e)


   def deal_CustomerService(self):
       '''
       处理顾客营销下的精准营销的数据 日期，店铺id，店铺名称
                                 潜在顾客（人数，客单价，近7天下单数）
                                 活跃顾客（人数，客单价，近7天下单数）
                                 沉默顾客（人数，客单价，近7天下单数）
                                 近30天门店新客（人数，客单价，近7天下单数）
                                 流失顾客（人数，客单价，近7天下单数）
                                 高价值顾客（人数，客单价，近7天下单数）
                                 低客单价顾客（人数，客单价，近7天下单数）
                                 差评顾客（人数，客单价，近7天下单数）
       :return:
       '''
       CustomerService_result = self.customerservice['result']
       try:
           for item in CustomerService_result:
               CustomerService = []
               groupName = item['groupName']
               people_num = item['statistics'][0]['value']
               people_mean_price = item['statistics'][1]['value']
               order_7_count = item['statistics'][2]['value']

               CustomerService.extend([str(self.yesterday),self.shopid,self.shopname,groupName,people_num,people_mean_price,order_7_count])
               print("运营数据分析的精准营销:",CustomerService)

               sql = "insert into elm_customer_precision_operation VALUES"
               batch = BatchSql(sql)
               batch.addBatch(CustomerService)
               self.db2.update(batch)
       except Exception as e:
           print('商家后台数据有问题：',e)



   def deal_getMarketingRecords(self):
       '''
       处理营销历史的数据，日期，店铺id，店铺名称，顾客群体，红包规则，使用量，投入产出比，操作时间
       :return:
       '''
       error = self.getMarketingRecords['error']
       if error == None:
           getMarketingRecords_result = self.getMarketingRecords['result']
           for item in getMarketingRecords_result:
               getMarketingRecords = []
               groupName = item['groupName']
               coupon_benefit = item['coupon']['benefit']
               coupon_condition = item['coupon']['condition']
               coupon_rule = '满'+str(coupon_condition)+'减'+str(coupon_benefit)
               marketingTime = item['marketingTime']

               getMarketingRecords.extend([str(self.yesterday),self.shopid,self.shopname,groupName,coupon_rule,marketingTime])
               print("营销历史的数据:",getMarketingRecords)

               sql = "insert into elm_marketingrecords_data VALUES"
               batch = BatchSql(sql)
               batch.addBatch(getMarketingRecords)
               self.db2.update(batch)


if __name__ == '__main__':

    pass



