import datetime
import re
import configparser
import os
import json
import pickle

def print_run_time(func):
    """
        装饰器，打印程序运行时间
    :param func:
    :return:
    """
    def wrapper(*args,**kw):
        starttime = datetime.datetime.now()
        print('程序开始')
        func(*args,**kw)
        endtime = datetime.datetime.now()
        print("程序结束共耗时：", (endtime - starttime).seconds)
    return wrapper


# 删除字符串特殊字符 py3.6
def remove_emoji(str, rep=''):
    return re.sub(u'[^\u4E00-\u9FA50-9a-zA-z·~！@#%&*×()（）\[\]【】}{\-—+➕=:：；\'\"“”‘’,，.。<>《》、?？/の]+', rep, str).strip()


# 删除字符串特殊字符 py2.7
def remove_emoji2(str):
    return re.sub(u'[^\u4E00-\u9FA50-9a-zA-z·~！@#%&*×()（）\[\]【】}{\-—+➕=:：；\'\"“”‘’,，.。<>《》、?？/の]+', '', str.decode('utf-8')).encode('utf-8').strip()


# 根据键获取map对象值
def getMapValue(item, key, rep=''):
    """
        获取dict类型的对于key的值
        :param item:
        :param key:
        :return:
    """
    try:
        value = str(item.get(key, "-999"))
        value = remove_emoji(value, rep)
        if value in ['', 'None']:
            value = '-999'  # 表示异常值
    except Exception:
        print("getMapValue出错！")
        print(type(item), '没有key:', key)
        value = "-999"
    return value


def saveJsonData(save_path,data):
    """
        保存json数据
    :param save_path: 保存路径，包含文件名
    :param data: json数据
    :return:
    """
    print(os.path.split(save_path))
    file_dir = os.path.split(save_path)[0]
    print(file_dir)
    if not os.path.isdir(file_dir):
        os.makedirs(file_dir)
    with open(save_path,'a') as f:
        f.write(json.dumps(data))
        f.write('\n')

def getJsonData(file_path):
    """
        获取保存的json数据
    :param file_path:
    :return:
    """
    arr = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                arr.append(json.loads(line))
    return arr

def showInGaoDeMap(data):
    """
        打印可以在高德地图展示的js代码
    :param data:
    :return:
    """
    position = ",".join(["{position:[%s,%s]}" % (s[0], s[1]) for s in data])
    print('[' + position + '];')


def getToday(format='%Y-%m-%d'):
    """
        获取今天日期的字符串
    :param format:
    :return:
    """
    return datetime.datetime.now().strftime(format)

def getTodayLater(day,format='%Y-%m-%d'):
    """
        获取今天以后几天的日期，天数可以为负
    :param day:
    :param format:
    :return:
    """
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=day)
    n_days = now - delta
    return n_days.strftime(format)


    
    
    