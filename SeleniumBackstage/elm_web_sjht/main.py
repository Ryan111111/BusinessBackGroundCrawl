from SeleniumBackstage.elm_web_sjht.WebClientCrawer import WebClientCrawer
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import traceback
from util.DB.DAO import DBUtils,BatchSql
import datetime

db = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'big_data', 'utf8mb4'))
sql = "insert into elm_backstage VALUES "
sql2 = "insert into rating_score VALUES"
sql3 = "insert into daily_activity_data VALUES"

def crawler(client,account,password):
    print("开始爬取商家：", account)
    result = []
    while True:
        try:
            params = client.getConfig('xpath.ini')  #获取xpath配置
            #获取登录url,进入登录页面
            login_url = params.get('login_url')[0]
            client.loginPage(login_url)

            #登录
            login_xpaths = params.get('login')
            rest_id = client.loginReturnId(account, password, login_xpaths)
            result.append(rest_id)
            result.append(client.date)

            # 获取数据下营业统计的数据
            client.switch_to_default_content()
            page_xpaths2 = params.get('page_yytj')
            client.gotoPage(page_xpaths2)
            iframe_xpaths = params.get('iframe')[3]
            client.goto_oneIFrame(iframe_xpaths)
            data_xpaths = params.get('data_yytj')
            yytj_data = client.getData(data_xpaths)
            print("营业统计：", yytj_data)

            #获取数据下流量分析的数据
            client.switch_to_default_content()
            page_xpaths = params.get('page')
            client.gotoPage(page_xpaths)
            iframe_xpaths = params.get('iframe')[0]
            client.goto_oneIFrame(iframe_xpaths)
            data_xpaths = params.get('data')
            result.extend(client.getData(data_xpaths))
            print(result)

            '''
            注意同一个列表下不用在点击根目录了
            每次需要退出账号
            '''

            # 获取数据下顾客分析的数据
            client.switch_to_default_content()
            page_xpaths = params.get('page_gkfx')
            client.gotoPage(page_xpaths)
            iframe_xpaths = params.get('iframe')[4]
            client.goto_oneIFrame(iframe_xpaths)
            page_xpaths2 = params.get('page_gkfx_to_data')
            client.gotoPage(page_xpaths2)
            data_xpaths = params.get('data_gkfx')
            gkfx_data = client.getData(data_xpaths)
            print("顾客总览：", gkfx_data)

            #获取顾客下顾客评价的数据
            # client.switch_to_default_content()
            # page_xpaths2 = params.get('page2')
            # client.gotoPage(page_xpaths2)
            # iframe_xpaths = params.get('iframe')[1]
            # client.goto_oneIFrame(iframe_xpaths)
            # data_xpaths = params.get('data2')
            # score = client.getData(data_xpaths)
            # print("用户评分：",score)

            #获取营销下的平台活动的数据
            client.switch_to_default_content()
            page_xpaths2 = params.get('page3')
            client.gotoPage(page_xpaths2)
            iframe_xpaths = params.get('iframe')[2]
            client.goto_oneIFrame(iframe_xpaths)
            page_xpaths2 = params.get('page3_canyu')
            client.gotoPage(page_xpaths2)
            try:
                data_xpaths = params.get('data3')
                dphd = client.getData(data_xpaths)
                data_xpaths = params.get('data3_detial')
                rules = client.getData(data_xpaths)

                print("店铺活动：",dphd)
                print("活动规则：",rules)
            except Exception as e:
                print("该店铺暂时没有商家活动！")


            #获取店铺推广下竞价推广的数据
            client.switch_to_default_content()
            page_xpaths2 = params.get('page_jjtg')
            client.gotoPage(page_xpaths2)
            iframe_xpaths = params.get('iframe')[5]
            client.goto_oneIFrame(iframe_xpaths)
            data_xpaths = params.get('data_jjtg')
            jjtg_data = client.getData(data_xpaths)
            print("竞价推广：", jjtg_data)

            # return result,score,dphd,rules

        except Exception as e:
            print('爬取店铺报错！，账号：%s,' % account)
            print(traceback.print_exc())
            time.sleep(10)

def deal_base_data(base_data):
    # 基本信息数据入库
    batch = BatchSql(sql)
    batch.addBatch(base_data)
    db.update(batch)

def deal_score_data(base_data,score):
    # 用户评论信息表的内容
    rating_data = []
    rating_data.append(str(base_data[0]))  # 店铺id
    rating_data.append(base_data[1])  # 日期
    rating_data.append(str('1'))  # 饿了么平台标签
    rating_data.append(str(score[0]))  # 用户评论
    print(rating_data)

    # 用户评分数据入库表
    batch = BatchSql(sql2)
    batch.addBatch(rating_data)
    db.update(batch)

def deal_comment_data(account,date,coupon_name,coupon_content):
    coupon_type = []
    """
    门店新用户立减：首
    满减活动：减
    折扣商品：折
    下单返券：返
    新用户立减活动：首
    商家优惠券：卷
    """
    for item in coupon_name:
        if item == '限量抢购-9.9元晚餐':
            coupon_type.append('抢')
        if item == '限量抢购-19.9元晚餐':
            coupon_type.append('抢')
        if item == '新用户立减':
            coupon_type.append('首')
    for i in range(len(coupon_name)):
        coupon_data = []
        name = coupon_name[i]
        type = coupon_type[i]
        content = coupon_content[i]
        coupon_data.extend([0, str(account), date, str(1), str(type), str(name), str(content)])
        print(coupon_data)

        batch = BatchSql(sql3)
        batch.addBatch(coupon_data)
        db.update(batch)

def run():
    '''
    调用配置文件中的账户信息，爬取数据并入库
    :return:
    '''
    client = WebClientCrawer()
    account = client.getAccount('account.ini')

    print('爬取数据开始')
    for account, password in account:
        result,score,dphd,rules = crawler(client,account,password)
        print("店铺数据：",result)

        date = datetime.datetime.now().strftime('%Y-%m-%d')
        #处理数据
        deal_base_data(result)
        deal_score_data(result,score[0])  #暂时只提供评分信息，满足web开发需求
        deal_comment_data(account,date,dphd,rules)

def auto_run():
    '''
    自动执行程序，每天的21：00
    :return:
    '''
    sched = BlockingScheduler()
    sched.add_job(run,
                  'cron',
                  hour=21,
                  minute=00,
                  end_date='2018-10-31')
    try:
        sched.start()  # 采用的是阻塞的方式，只有一个线程专职做调度的任务
    except (KeyboardInterrupt, SystemExit):
        print('Exit The Job!')
        sched.shutdown()

if __name__ == '__main__':
    run()
    # auto_run()
