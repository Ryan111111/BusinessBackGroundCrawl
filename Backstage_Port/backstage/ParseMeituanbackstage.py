import datetime
from util.DB.DAO import DBUtils,BatchSql

class PaerseMeituanbackstage():
   '''
   解析美团商家后台数据，并写入相应的数据库
   '''
   def __init__(self,commentSummary,businessStatistics,TrafficStats,
                customerAnalysis,customerPerfer,effectiveOrders,
                todaystatistics,activitylist,customer_reminderInfo,
                customer_reminder,history_consume,
                PeersCompareAnalysis, getCouponLabel,
                shopid):
        self.commentSummary = commentSummary
        self.businessStatistics = businessStatistics
        self.TrafficStats = TrafficStats
        self.customerAnalysis = customerAnalysis
        self.customerPerfer = customerPerfer
        self.effectiveOrders = effectiveOrders
        self.todaystatistics = todaystatistics
        self.activitylist = activitylist
        self.customer_reminderInfo = customer_reminderInfo
        self.customer_reminder = customer_reminder
        self.history_consume = history_consume
        self.PeersCompareAnalysis = PeersCompareAnalysis
        self.getCouponLabel = getCouponLabel
        self.shopid = shopid

        self.db = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'big_data', 'utf8mb4'))
        self.db2 = DBUtils(('192.168.1.200', 3306, 'njjs_zsz', 'njjs1234', 'zszdata', 'utf8mb4'))
        self.db3 = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'wmds', 'utf8mb4'))
        self.date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.yesterday = datetime.date.today() - datetime.timedelta(days=1)
        self.time = datetime.datetime.now().strftime('%H:%M:%S')
        self.platform = 2  #平台标签
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
       '''
       获取店铺id对应的名称
       :param shopid:店铺id
       :return:
       '''
       if shopid == 'wmxhxq1392':
           shopname = '谢恒兴奇味鸡煲(义乌店)'
           return shopname
       if shopid == 'wmxhxq168673':
           shopname = '谢恒兴奇味鸡煲(河西万达店))'
           return shopname
       if shopid == 'wmxhxq1791':
           shopname = '谢恒兴奇味鸡煲(明发广场店)'
           return shopname
       if shopid == 'wmxhxq18423':
           shopname = '谢恒兴奇味鸡煲(元通店)'
           return shopname
       if shopid == 'wmxhxq19610':
           shopname = '谢恒兴奇味鸡煲(油坊桥店)'
           return shopname
       if shopid == 'wmxhxq20715':
           shopname = '谢恒兴奇味鸡煲(四方新村店)'
           return shopname

   def WMDS_meituan_data(self):
       '''
       处理美团外卖大师的数据
       :return:
       '''
       WMDS_meituan_data = []
       m_bizScore = self.commentSummary['data']['bizScore']
       m_exposureNum = self.TrafficStats['data']['flowGeneralInfoVo']['exposureNum']
       m_visitNum = self.TrafficStats['data']['flowGeneralInfoVo']['visitNum']
       m_orderNum = self.TrafficStats['data']['flowGeneralInfoVo']['orderNum']
       m_visitRate = self.TrafficStats['data']['flowGeneralInfoVo']['visitRate']
       m_orderRate = self.TrafficStats['data']['flowGeneralInfoVo']['orderRate']

       WMDS_meituan_data.extend([self.WMDS_shopid,m_bizScore,m_exposureNum,m_visitNum,m_orderNum,m_visitRate,m_orderRate])
       print('美团外卖大师的数据：',WMDS_meituan_data)
       return WMDS_meituan_data

   '''
   处理web端的商家后台数据，每天晚上8-9点运行，四张表的数据
   '''
   def deal_commentSummary(self):
        '''
        处理顾客评价的信息：店铺ID、时间、平台、评分、评分日比
        :return:
        '''
        commentSummary_content = []
        bizScore = self.commentSummary['data']['bizScore']
        commentSummary_content.extend([self.shopid,self.date,self.platform,bizScore])
        print('店铺评分信息：',commentSummary_content)

        sql = "insert into rating_score VALUES "
        batch = BatchSql(sql)
        batch.addBatch(commentSummary_content)
        self.db.update(batch)

   def deal_TrafficStats(self):
       '''
       处理数据流量分析的信息：店铺id、时间、曝光人数，访问人数，下单人数，访问转化率，下单转化率
       :return:
       '''
       TrafficStats = []
       exposureNum = self.TrafficStats['data']['flowGeneralInfoVo']['exposureNum']
       visitNum = self.TrafficStats['data']['flowGeneralInfoVo']['visitNum']
       orderNum = self.TrafficStats['data']['flowGeneralInfoVo']['orderNum']
       visitRate = self.TrafficStats['data']['flowGeneralInfoVo']['visitRate']
       orderRate = self.TrafficStats['data']['flowGeneralInfoVo']['orderRate']
       TrafficStats.extend([self.shopid,self.date,exposureNum,visitNum,orderNum,visitRate,orderRate])

       print('流量分析信息：', TrafficStats)
       sql = "insert into meituan_backstage VALUES "
       batch = BatchSql(sql)
       batch.addBatch(TrafficStats)
       self.db.update(batch)

   def deal_Activity_type(self,type):
       '''
       处理活动信息的类型问题
       :param type:
       :return:
       '''
       if type == 17:
           type = '折'
           return type
       if type == 100:
           type = '返'
           return type
       if type == 103:
           type = '领'
           return type
       if type == 2:
           type = '减'
           return type
       if type == 1:
           type = '首'
           return type
       if type == 22:
           type = '新'
           return type
       else:
           type = '惠'
           return type

   def deal_Activitylist(self):
       '''
       处理店铺活动的信息：id，店铺id，活动id，时间，平台名称，活动类型，活动名称，活动内容
       :return:
       '''
       Activity_onGoingActs = self.activitylist['data']['onGoingActs']
       for item in Activity_onGoingActs:
           Activitylist = []
           actId = item['actId']
           actName = item['actName']
           poiPolicy = item['poiPolicy']

           type = item['type']
           Activity_type = self.deal_Activity_type(type)
           Activitylist.extend([0,self.shopid,actId,self.date,2,Activity_type,actName,poiPolicy])

           print('店铺活动信息：', Activitylist)
           sql = "insert into daily_shop_activity_data VALUES"
           batch = BatchSql(sql)
           batch.addBatch(Activitylist)
           self.db3.update(batch)

   def deal_CustomerReminder(self):
       '''
       处理店铺催单信息：id，店铺id，日期，时间，平台名称，催单次数，催单时间
       :return:
       '''
       CustomerReminder_Info = self.customer_reminderInfo['wmOrderList']
       CustomerReminder_Data = self.customer_reminder['data']
       if CustomerReminder_Info != None:
           for item in CustomerReminder_Info:                                  #获取另一个催单账号信息借口获取催单的ID
               CustomerReminder_ID = item['wm_order_id_view']
               try:
                   for item in CustomerReminder_Data[str(CustomerReminder_ID)]:    #根据催单ID获取相应的催单的时间信息
                       times = len(CustomerReminder_Data[str(CustomerReminder_ID)])  #催单的次数
                       CustomerReminder = []
                       reminder_time_fmt = item['reminder_time_fmt']
                       response_time_fmt = item['response_time_fmt']
                       reminder_time_fmt = str(reminder_time_fmt).split(':')[1]
                       response_time_fmt = str(response_time_fmt).split(':')[1]
                       reminder_time = int(response_time_fmt)-int(reminder_time_fmt)   #商家后台恢复催单信息的时长
                       CustomerReminder.extend([0,self.shopid,self.date,self.time,CustomerReminder_ID,2,times,reminder_time])
                       print('客户催单信息：',CustomerReminder)

                       sql = "insert into daily_customer_reminder VALUES"
                       batch = BatchSql(sql)
                       batch.addBatch(CustomerReminder)
                       self.db.update(batch)
               except Exception:
                   pass
                   # print('催单信息有问题！')



   '''
   运营日报的商家后台数据
   '''
   def deal_meituan_daily_operational_data(self):
       '''
       运营日报数据入库：id,店铺id，店铺名称，日期，
                      日有效订单量，订单收入，
                      曝光量，进店量，下单量，进店转化率，下单转化绿
                      新客人数，新客占比，旧客人数，旧客占比
                      推广资金，(推广点击次数，单词点击费用)
       :return:
       '''
       meituan_daily_operational_data = []
       effectiveOrders = self.effectiveOrders['data']['effectiveOrders']
       settleAcc = self.effectiveOrders['data']['settleAcc']

       exposureNum = self.TrafficStats['data']['flowGeneralInfoVo']['exposureNum']
       visitNum = self.TrafficStats['data']['flowGeneralInfoVo']['visitNum']
       xiadanNum = self.TrafficStats['data']['flowGeneralInfoVo']['orderNum']
       visitRate = self.TrafficStats['data']['flowGeneralInfoVo']['visitRate']
       orderRate = self.TrafficStats['data']['flowGeneralInfoVo']['orderRate']

       newOrderNum = self.customerAnalysis['data']['newOrderNum']
       oldOrderNum = self.customerAnalysis['data']['oldOrderNum']
       orderNum = self.customerAnalysis['data']['orderNum']
       if orderNum == 0:
           newOrderNum_percent = 0
           oldOrderNum_percent = 0
       else:
           newOrderNum_percent = newOrderNum / orderNum
           oldOrderNum_percent = oldOrderNum / orderNum

       history_consume_list = self.history_consume['data']['flowVoList']
       popularize_amount = 0
       for item in history_consume_list:
           history_consume_list_date = str(item['date']).split(' ')[0]
           history_consume_list_reason  = item['reason']
           if history_consume_list_date == str(self.yesterday) and history_consume_list_reason == '推广消费':
               popularize_amount += abs(item['amount'])
               # print(popularize_amount)

       meituan_daily_operational_data.extend([0,self.shopid,self.shopname,str(self.yesterday),
                                              effectiveOrders,settleAcc,
                                              exposureNum,visitNum,xiadanNum,visitRate,orderRate,
                                              newOrderNum,newOrderNum_percent,oldOrderNum,oldOrderNum_percent,
                                              popularize_amount])
       print("美团运营日报数据:",meituan_daily_operational_data)
       sql = "insert into meituan_daily_operational_data VALUES"
       batch = BatchSql(sql)
       batch.addBatch(meituan_daily_operational_data)
       self.db2.update(batch)

   '''
    商家后台数据分析
   '''
   def deal_meituan_Operational_data_analysis(self):
       '''
       处理商家后台数据分析的数据：id，店铺id，店铺名称，日期，
                              有效订单，订单收入，
                              有效订单打败同行，订单收入打败同行，
                              下单人数，新客人数，老客人数，
                              曝光人数，访问人数，下单人数，访问转化率，下单转化率，
                              商家评分，口味评分，包装评分，配送评分，
       :return:
       '''
       meituan_Operational_data_analysis = []
       effectiveOrders = self.effectiveOrders['data']['effectiveOrders']
       settleAcc = self.effectiveOrders['data']['settleAcc']

       effectiveOrdersRankPercent = self.PeersCompareAnalysis['data']['effectiveOrdersRankPercent']
       settleAccRankPercent = self.PeersCompareAnalysis['data']['settleAccRankPercent']

       newOrderNum = self.customerAnalysis['data']['newOrderNum']
       oldOrderNum = self.customerAnalysis['data']['oldOrderNum']
       orderNum = self.customerAnalysis['data']['orderNum']

       exposureNum = self.TrafficStats['data']['flowGeneralInfoVo']['exposureNum']
       visitNum = self.TrafficStats['data']['flowGeneralInfoVo']['visitNum']
       xiadanNum = self.TrafficStats['data']['flowGeneralInfoVo']['orderNum']
       visitRate = self.TrafficStats['data']['flowGeneralInfoVo']['visitRate']
       orderRate = self.TrafficStats['data']['flowGeneralInfoVo']['orderRate']

       bizScore = self.commentSummary['data']['bizScore']
       tasteScore = self.commentSummary['data']['tasteScore']
       packScore = self.commentSummary['data']['packScore']
       deliveryScore = self.commentSummary['data']['deliveryScore']

       meituan_Operational_data_analysis.extend([0,self.shopid,self.shopname,str(self.yesterday),
                                                 effectiveOrders,settleAcc,
                                                 effectiveOrdersRankPercent,settleAccRankPercent,
                                                 orderNum,newOrderNum,oldOrderNum,
                                                 exposureNum,visitNum,xiadanNum,visitRate,orderRate,
                                                 bizScore,tasteScore,packScore,deliveryScore])
       print('商家后台数据分析：',meituan_Operational_data_analysis)

       sql = "insert into meituan_Operational_data_analysis VALUES"
       batch = BatchSql(sql)
       batch.addBatch(meituan_Operational_data_analysis)
       self.db2.update(batch)


   def deal_meituan_customer_prefer_analysis(self):
       '''
       处理同行顾客爱好：id,店铺id，店铺名称，日期
                      同行顾客爱好的菜品名，该菜品的同行百分比
       :return:
       '''
       customer_prefer_analysi = self.customerPerfer['data']['pubCusEat']
       for item in customer_prefer_analysi:
           meituan_customer_prefer_analysi = []
           customer_prefer_name = item[0]
           customer_prefer_percent = item[1]

           meituan_customer_prefer_analysi.extend([0,self.shopid,self.shopname,str(self.yesterday),
                                                   customer_prefer_name,float(customer_prefer_percent)])
           print('同行顾客爱好：',meituan_customer_prefer_analysi)

           sql = "insert into meituan_customer_prefer_analysis VALUES"
           batch = BatchSql(sql)
           batch.addBatch(meituan_customer_prefer_analysi)
           self.db2.update(batch)


   def deal_customer_prefer_activity_type(self,pic):
       '''
       处理顾客喜欢的活动的类型
       :param pic:
       :return:
       '''
       if pic == 'http://p1.meituan.net/xianfu/9c997ecce6150671b8459738a26f8bd9767.png':
           type = '折'
           return type
       if pic == 'http://p0.meituan.net/xianfu/652eea4034250563fe11b02e3219ba8d981.png':
           type = '返'
           return type
       if pic == 'http://p0.meituan.net/coupon/1ae419cdb421cfdeb3575fb90e1cc340862.png@!style1':
           type = '卷'
           return type
       if pic == 'http://p0.meituan.net/xianfu/f8bc8dffdbc805878aa3801a33f563cd1001.png':
           type = '减'
           return type
       if pic == 'http://p1.meituan.net/xianfu/5ffe01c550a139db693d152cefd1b247869.png':
           type = '赠'
           return type
       else:
           type = '活动'
           return type


   def deal_meituan_customer_prefer_activity(self):
       '''
       处理同行顾客喜欢的活动：id，店铺id，店铺名称，日期，
                           同行顾客喜欢的活动，活动类型

       :return:
       '''
       customer_prefer_activity = self.customerPerfer['data']['pubCusAct']
       for item in customer_prefer_activity:
           customer_prefer_activity = []
           customer_prefer_activity_name = item[0]
           customer_prefer_activity_pic = item[1]
           customer_prefer_activity_type = self.deal_customer_prefer_activity_type(customer_prefer_activity_pic)
           customer_prefer_activity.extend([0,self.shopid,self.shopname,str(self.yesterday),
                                            customer_prefer_activity_name,customer_prefer_activity_type])
           print("顾客喜欢的活动：",customer_prefer_activity)

           sql = "insert into meituan_customer_prefer_activity VALUES"
           batch = BatchSql(sql)
           batch.addBatch(customer_prefer_activity)
           self.db2.update(batch)



   def deal_meituan_customer_precision_operation(self):
       '''
       处理精准营销的数据：id，店铺id，店铺名称，日期，
                        营销名称，营销活动人数
       :return:
       '''
       customer_precision_labelList = self.getCouponLabel['data']['labelList']
       for item in customer_precision_labelList:
           customer_precision_operation = []
           precision_operation_name = item['title']
           precision_operation_numbers = item['nums']

           customer_precision_operation.extend([0,self.shopid,self.shopname,str(self.yesterday),
                                                precision_operation_name,precision_operation_numbers])
           print('精准营销的活动信息：',customer_precision_operation)

           sql = "insert into meituan_customer_precision_operation VALUES"
           batch = BatchSql(sql)
           batch.addBatch(customer_precision_operation)
           self.db2.update(batch)


if __name__ == '__main__':
   pass

