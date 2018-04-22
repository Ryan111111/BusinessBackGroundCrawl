import os
import json


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




    
    
    