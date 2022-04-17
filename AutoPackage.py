# coding=UTF-8<code>
import os
import sys
import time

# 1.返回yml所在目录
def get_yml(dir, packge):
    # 判断目录是否存在
    if os.path.exists(dir):
        print('目录存在')
        # 目录存在查询该目录下所有文件与文件夹
        findAll = os.listdir(dir)
        # 遍历
        for item in findAll:
            findAllPATH = os.path.join(dir, item)
            # 如果是文件
            if os.path.isfile(findAllPATH):
                # 判断文件是否包含指定字符串,!= -1证明找到了包含指定字符串的文件
                if item.find(str(packge)) != -1:
                    return findAllPATH
            elif os.path.isdir(findAllPATH):
                return get_yml(findAllPATH, packge)

# 2.查询上传时间为最新的包
def new_packge(packge):
    # 1.1 定义一个空列表
    list1 = []
    # 在images目录下查询上传时间为最新的包
    dir_image = '/home/airc/images'
    # 判断目录是否存在
    if os.path.exists(dir_image):
        # 查询该目录下所有文件
        listdir = os.listdir(dir_image)
        # 遍历所有文件与文件夹
        for lidir in listdir:
            if os.path.isfile(os.path.join(dir_image, lidir)):
                if lidir.find(str(packge)) != -1:
                    list1.append(lidir)
    # 4.对列表中的时间进行排序
    list1.sort(key=lambda fn: os.path.getctime(dir_image + '/' + fn))
    return list1[-1]

if __name__ == '__main__':
    str1 = sys.argv[1]
    dir = '/home/airc/compose'
    # 返回yml路径
    yml = get_yml(dir, str1)
    print('yml路径为:%s' % yml)
    # 返回上传时间为最新的包名称
    newpackge = new_packge(str1)
    print('更新的包名称为:%s' %newpackge)
    # 停止服务
    if os.system('docker-compose -f %s down' % yml) == 0:
        print('=====================服务停止成功!!!=======================')
    else:
        print('=====================服务停止失败!!!=======================')

    # 获取镜像id
    images_id = """docker images|grep %s| awk -F" " '{print $3}'""" % str1
    reslut = os.popen(images_id).readlines()
    str3 = reslut[0]

    if os.system('docker rmi %s' % str3) == 0:
        print('=====================移除镜像id成功!=======================')
    else:
        print('=====================移除镜像id失败!=======================')
    if os.system('docker load -i %s' % newpackge) == 0:
        print('加载的镜像包为:%s!' % newpackge)
    else:
        print('========================镜像包加载失败======================')
    if os.system('docker-compose -f %s up -d' % yml) == 0:
        print('======================服务启动成功!========================')
    time.sleep(3)
    if os.system('docker logs -f %s --tail 200' % str1) == 0:
        print('======================日志加载成功!========================')