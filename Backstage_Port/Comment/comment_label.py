import jieba
import datetime
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from util.DB.DAO import DBUtils,BatchSql
"""
分词对应列表信息：漏餐：1 异物：2 夹生饭：3 变质：4 无差评：0
"""

def get_config():
    path = 'data_test.txt'               ##读取分词配置文件路径
    jieba.load_userdict("newdict.txt")   #自定义的jieba词典
    data1 = pd.read_table(path,header=None,encoding='utf-8')
    data = pd.DataFrame(data1)
    all_data = []
    key_vaule = data.iloc[:,1:4:2]                                          #读取相应的分词的关键词，取出相对应的匹配标签
    for i in range(len(data)):
        all_data.append(str(data.iloc[i,0]))
    return key_vaule

def get_rating_info(date):
    '''
    获取当天的评论，一次性从数据库中读取出来，不断产生评论数据
    :param date:
    :return: 满足要求的评论数据
    '''
    db = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'compass_prod', 'utf8mb4'))
    sql = "select t1.id,t1.shop_id,t1.rate_content from order_comment t1 where t1.created_at like %s and t1.rate_content != ''"
    date = date+'%'
    result = db.queryForList(sql,[date])
    return result

def rate_analyze(id,comment_shopid,date,comment_content):
    '''
    对评论数据进行分词识别出相应的类别，然后写入数据库中
    :param id: 评论ID
    :param date: 当前日期
    :param comment_content:评论的内容
    :return:
    '''
    time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    key_vaule = get_config()
    db2 = DBUtils(('116.62.70.68', 3306, 'bigdata', 'gisfood20171220@nj', 'big_data', 'utf8mb4'))
    sql2 = "insert into comment_flag VALUES"
    seg_list = jieba.cut(comment_content)   #利用jieba分词，取出句子中所有的分词内容
    print("评论内容：",comment_content)
    flag = 0                                #标志位，0：分词中没有差评信息  1：分词中存在差评信息
    keywords = ' '
    for item in seg_list:                   #对一条评论的分词结果进行匹配
        for j in range(len(key_vaule)):     #在词库中进行配对
            if key_vaule.iloc[j,0]==item:
                print("有差评内容:",item)
                keywords = item
                comment_class = int(key_vaule.iloc[j,1])  #将评论的标签置为相应的值
                flag = 1
                break
        if flag ==0 :
            keywords = r' '
            comment_class = 0                             #没有差评的时候标签置为0

    print("评论ID："+str(id),"所属分类："+str(comment_class))
    batch = BatchSql(sql2)                                #写入数据库
    batch.addBatch([0,id,comment_shopid,date,comment_class,keywords,time,None])
    db2.update(batch)

def deal_comment():
    '''
    主程序，从数据库中获取今天的评论数据，然后一条一条进行结巴分词，判断分类，然后写入数据库
    :return:
    '''
    date = str(datetime.datetime.now().strftime('%Y-%m-%d'))  #获取当前的日期
    # date = '2018-01-22'
    today_rating = get_rating_info(date)
    for item in today_rating:
        print(item)
        comment_id = item[0]
        comment_shopid = item[1]
        comment_content = item[2]
        rate_analyze(comment_id,comment_shopid,date,comment_content)


if __name__ == '__main__':
    # pass
    # auto_run()
    deal_comment()

