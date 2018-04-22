from apscheduler.schedulers.blocking import BlockingScheduler
from Backstage_Port.backstage.MeituanAPI import meituan_web_crawl,\
    meituan_daily_and_operation_analysis
from Backstage_Port.backstage.ElmAPIManager import elm_operation_crawl,elm_web_crawl
from Backstage_Port.Comment.comment_label import deal_comment
from Backstage_Port.backstage.MeituanAPI import deal_WMDS_meituan_data
from Backstage_Port.backstage.ElmAPIManager import deal_WMDS_elm_data
import datetime
from util.DB.DAO import DBUtils

yesterday = datetime.date.today() - datetime.timedelta(days=1)
db = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'wmds', 'utf8mb4'))

def deal_WMDS_data():
    '''
    解析两个平台的数据
    :return:
    '''

    WMDS_meituan_data = deal_WMDS_meituan_data()
    WMDS_elm_data = deal_WMDS_elm_data()
    elm_shop_data = []
    meituan_shop_data = []
    elm_shop_data.extend([WMDS_elm_data[0],WMDS_elm_data[1],WMDS_elm_data[2],WMDS_elm_data[3],WMDS_elm_data[4],[16,0,0,0,0,0,0],[17,0,0,0,0,0,0],[18,0,0,0,0,0,0],WMDS_elm_data[5]])
    meituan_shop_data.extend([WMDS_meituan_data[0],WMDS_meituan_data[1],[13,0,0,0,0,0,0],[14,0,0,0,0,0,0],WMDS_meituan_data[2],[16,0,0,0,0,0,0],WMDS_meituan_data[3],WMDS_meituan_data[4],[19,0,0,0,0,0,0]])

    for i in range(0,9):
        elm_data = elm_shop_data[i]
        meituan_data = meituan_shop_data[i]
        print('美团数据：',meituan_data)
        print('饿了么数据：',elm_data)
        deal_WMDS_data_all(elm_data,meituan_data,)


def deal_WMDS_data_all(elm_data,meituan_data):
    '''
    更新商家后台的数据，根据日期和店铺id来update数据表
    :param elm_data:
    :param meituan_data:
    :return:
    '''
    try:
        WMDS_web_data =[]
        shopid = elm_data[0]
        e_Score = float('%.2f'%float(elm_data[1]))
        e_exposureTotalCount = elm_data[2]
        e_visitorNum = elm_data[3]
        e_buyerNum = elm_data[4]
        e_visitorConversionRate = float('%.2f'%float(elm_data[5]))
        e_buyerConversionRate = float('%.2f'%float(elm_data[6]))

        m_Score = float('%.1f'%float(meituan_data[1]))
        m_exposureTotalCount = meituan_data[2]
        m_visitorNum = meituan_data[3]
        m_buyerNum = meituan_data[4]
        m_visitorConversionRate = float('%.2f'%float(meituan_data[5]))
        m_buyerConversionRate = float('%.2f'%float(meituan_data[6]))
        flow_bgl_all = e_exposureTotalCount+m_exposureTotalCount
        flow_ddrs_all = e_visitorNum + m_visitorNum
        flow_xdrs_all = e_buyerNum + m_buyerNum
        flow_jdzhl_all = float('%.2f'%float(e_visitorConversionRate + m_visitorConversionRate))
        flow_xdzhl_all = float('%.2f'%float(e_buyerConversionRate + m_buyerConversionRate))

        WMDS_web_data.extend([0, shopid, str(yesterday),
                              flow_bgl_all, flow_jdzhl_all, flow_ddrs_all, flow_xdzhl_all, flow_xdrs_all,
                              e_exposureTotalCount, e_visitorConversionRate, e_visitorNum, e_buyerConversionRate,e_buyerNum,
                              m_exposureTotalCount, m_visitorConversionRate, m_visitorNum, m_buyerConversionRate,m_buyerNum,
                              e_Score, m_Score])

        print("外卖大师的数据：",WMDS_web_data)
        sql = """
                  update daily_total_data
                  set flow_bgl_all = %s,
                      flow_jdzhl_all = %s,
                      flow_ddrs_all = %s,
                      flow_xdzhl_all = %s,
                      flow_xdrs_all = %s,
                      flow_bgl_elm = %s,
                      flow_jdzhl_elm = %s,
                      flow_ddrs_elm = %s,
                      flow_xdzhl_elm = %s,
                      flow_xdrs_elm = %s,
                      flow_bgl_mt = %s,
                      flow_jdzhl_mt = %s,
                      flow_ddrs_mt = %s,
                      flow_xdzhl_mt = %s,
                      flow_xdrs_mt = %s,
                      score_val_elm = %s,
                      score_val_mt = %s
                  where date = '%s' and shop_id = %s;
                  """%(flow_bgl_all, flow_jdzhl_all, flow_ddrs_all, flow_xdzhl_all, flow_xdrs_all,
                   e_exposureTotalCount, e_visitorConversionRate, e_visitorNum, e_buyerConversionRate,e_buyerNum,
                   m_exposureTotalCount, m_visitorConversionRate, m_visitorNum, m_buyerConversionRate,m_buyerNum,
                   e_Score, m_Score,
                   str(yesterday),shopid)
        print(sql)
        db.deal_sql(sql)
    except Exception as e:
        print(e)

def auto_run():
    '''
    5个定时任务：
    elm运营日报和数据分析：9；00定时爬取
    美团 运营日报和数据分析：9:10定时爬取
    elm web端数据爬取：20:30定时爬取
    美团 web端数据爬取：20:40定时爬取
    web端端上商家后台数据：20：00定时爬取
    商家后台评论数据分类
    :return:
    '''
    sched = BlockingScheduler()
    sched.add_job(deal_WMDS_data, 'cron', hour=8, minute=00, end_date='2018-10-31')
    sched.add_job(elm_operation_crawl,'cron',hour=9,minute=00,end_date='2018-10-31')
    sched.add_job(meituan_daily_and_operation_analysis,'cron',hour=9,minute=10,end_date='2018-10-31')
    sched.add_job(elm_web_crawl, 'cron', hour=20, minute=30, end_date='2018-10-31')
    sched.add_job(meituan_web_crawl,'cron',hour=20,minute=40,end_date='2018-10-31')
    sched.add_job(deal_comment,'cron',hour=20, minute=50, end_date='2018-10-31')


    try:
        sched.start()  # 采用的是阻塞的方式，只有一个线程专职做调度的任务
    except (KeyboardInterrupt, SystemExit):
        print('Exit The Job!')
        sched.shutdown()



if __name__ == "__main__":
    # pass
    # auto_run()

    deal_WMDS_data()
    # elm_operation_crawl()
    # meituan_daily_and_operation_analysis()

    # elm_web_crawl()
    # meituan_web_crawl()
    # deal_comment()

