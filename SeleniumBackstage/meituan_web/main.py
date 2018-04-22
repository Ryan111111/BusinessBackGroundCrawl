from SeleniumBackstage.meituan_web.Clientlogin import Clientlogin
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import datetime
import traceback
from util.DB.DAO import DBUtils,BatchSql

db = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'big_data', 'utf8mb4'))
sql = "insert into meituan_backstage VALUES "
sql2 = "insert into rating_score VALUES"
sql3 = "insert into daily_activity_data VALUES"

def crawler(client,account,password):
    """
    爬取商家后台的数据
    :param client:  浏览器相应操作
    :param account: 用户账号
    :param password:用户密码
    :return:
    """
    print("开始爬取商家：", account)
    #分别保存每组爬取的数据
    result = []
    result1 = []
    result1_1 = []
    result2 = []
    coupon_name = []
    coupon_content = []
    rating_data = []

    while True:
        try:
            params = client.getConfig('xpath.ini')  #获取xpath配置

            """
            爬取第一组门店推广的信息
            数据：曝光提升数，访问提升数，推广消费，单词访问消费
            """
            print("开始爬取第一组数据>>>")
            page_xpaths = params.get('page')[0]
            client.gotoPage(page_xpaths)
            iframe_xpaths = params.get('iframe')[1]
            client.gotoIFrame(iframe_xpaths)
            data_xpaths = params.get('data')
            result.extend(client.getData(data_xpaths))
            print("获取的第一组数据：",result)

            """
            爬取第二组数据
            数据：曝光人数，曝光次数，访问人数，访问次数，下单人数，下单次数
            """
            print("开始爬取第二组数据>>>")
            client.switch_to_default_content()        #返回主文档
            page_xpaths = params.get('page')[1]
            client.gotoPage(page_xpaths)
            page_xpaths = params.get('page')[2]
            client.gotoPage(page_xpaths)
            iframe_xpaths = params.get('iframe')[1]
            client.gotoIFrame(iframe_xpaths)
            #对第二组数据进行处理
            data_xpaths = params.get('data2')
            result1.extend(client.getData(data_xpaths))
            for item in range(len(result1)):
                item = str(result1[item])
                result2.append(item.split("：")[1])

            data_xpaths = params.get('data2-1')
            result1_1.extend(client.getData(data_xpaths))
            for item in range(len(result1_1)):
                item = str(result1_1[item])
                result2.append(item.split("%")[0])

            print("获取的第二组数据：",result2)

            result.extend(result2)

            """
            爬取第三组数据
            店铺优惠卷信息可能有可能没有，需要进行判断
            """
            print("开始爬取第三组数据>>>")
            client.switch_to_default_content()
            page_xpaths = params.get('page')[3]
            client.gotoPage(page_xpaths)
            page_xpaths = params.get('page')[4]
            client.gotoPage(page_xpaths)
            iframe_xpaths = params.get('iframe')[1]
            client.gotoIFrame(iframe_xpaths)

            #判断是否有商家活动信息，有即返回，没有不进行
            try:
                data_xpaths = params.get('data4')
                coupon_name.extend(client.getData(data_xpaths))
                data_xpaths = params.get('data4_info')
                coupon_content.extend(client.getData(data_xpaths))
                print("店铺活动信息：",coupon_name)
            except Exception as e:
                #该店铺没有优惠卷信息，输出值都设置为[0,0,0,0,0,0,0]
                print("账号：%s,暂时没有店铺活动!" %account)

            """
            爬去第四组数据：商家评分信息
            """
            print("开始爬取第四组数据>>>")
            client.switch_to_default_content()  # 返回主文档
            page_xpaths = params.get('page')[7]
            client.gotoPage(page_xpaths)
            page_xpaths = params.get('page')[8]
            client.gotoPage(page_xpaths)
            iframe_xpaths = params.get('iframe')[1]
            client.gotoIFrame(iframe_xpaths)
            data_xpaths = params.get('data5')
            score = client.getData(data_xpaths)

            date = datetime.datetime.now().strftime('%Y-%m-%d')
            rating_data.extend([str(account),date,str(2),str(score[0])])
            print(rating_data)

            """
            推出账号操作，不影响下一次数据爬取的操作
            """
            print("退出当前账号>>>")
            # 返回主文档
            client.switch_to_default_content()
            page_xpaths = params.get('page_close')[0]
            client.gotoPage(page_xpaths)
            page_xpaths = params.get('page_close')[1]
            client.gotoPage(page_xpaths)
            print("已推出当前账号！")

            return result,rating_data,coupon_name,coupon_content
        except Exception as e:
            print('爬取店铺报错！，账号：%s,' % account)
            print(traceback.print_exc())
            time.sleep(10)

def deal_coupon_data(account,date,coupon_name,coupon_content):
    '''
    处理店铺活动信息
    :param account:
    :param date:
    :param coupon_name:
    :param coupon_content:
    :return:
    '''
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
        if item == '折扣商品':
            coupon_type.append('折')
        if item == '下单返券':
            coupon_type.append('返')
        if item == '门店新用户立减':
            coupon_type.append('首')
        if item == '商家优惠券':
            coupon_type.append('卷')
        if item == '满减活动':
            coupon_type.append('减')
        if item == '新用户立减活动':
            coupon_type.append('首')
    for i in range(len(coupon_name)):
        coupon_data = []
        name = coupon_name[i]
        type = coupon_type[i]
        content = coupon_content[i]
        coupon_data.extend([0,str(account),date,str(2),str(type),str(name),str(content)])
        print(coupon_data)

        batch = BatchSql(sql3)
        batch.addBatch(coupon_data)
        db.update(batch)


def deal_base_data(account,date,result):
    '''
    处理基础的几条信息
    :param account:
    :param date:
    :param result:
    :return:
    '''
    base_data = []
    base_data.extend([account,date,result[4],result[6],result[8],result[10],result[11]])
    print("处理数据：",base_data)
    batch = BatchSql(sql)
    batch.addBatch(base_data)
    db.update(batch)

def deal_score_data(score):
    '''
    处理商家评分信息
    :param score:
    :return:
    '''
    print("处理数据",score)
    batch = BatchSql(sql2)
    batch.addBatch(score)
    db.update(batch)

def run():
    #先导入账号密码信息，更新cookie内容
    clientlogin = Clientlogin()
    account= clientlogin.getAccount('account.ini')
    #对ini文件中的所有账号进行数据爬取
    for account, password in account:
        params = clientlogin.getConfig('xpath.ini')  # 获取xpath配置
        #获取登录url,进入登录页面
        login_url = params.get('login_url')[0]
        clientlogin.loginPage(login_url)
        #切换iframe到登陆界面
        iframe_xpaths = params.get('iframe')[0]
        clientlogin.gotoIFrame(iframe_xpaths)
        #登录
        login_xpaths = params.get('login')
        clientlogin.loginReturnId(account,password,login_xpaths)


        date = clientlogin.date                                #获取当前爬取数据的时间
        result,score,coupon_name,coupon_content = crawler(clientlogin,account,password)         #返回爬取的数据

        deal_base_data(account,date,result)
        deal_score_data(score)
        deal_coupon_data(account,date,coupon_name,coupon_content)

        # 关闭浏览器，为下次免登录做准备,必须sleep2秒出现登录页面才不会影响下次登录
        time.sleep(2)
        clientlogin.quit_browser()

def auto_run():
    """
    自动启动设置，每天的21：00
    :return:
    """
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
    # coupon_name = ['折扣商品', '下单返券', '门店新用户立减', '商家优惠券', '满减活动', '新用户立减活动']
    # coupon_content = ['折扣商品', '下单返券', '门店新用户立减', '商家优惠券', '满减活动', '新用户立减活动']
    # deal_coupon_data('123','2018-10-31',coupon_name,coupon_content)


