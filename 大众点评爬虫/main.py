# -*- coding: utf-8 -*-
"""
Created on Mon Jul  9 16:42:52 2018

@author: bin
"""

#目标爬取店铺的评论

import requests
from bs4 import BeautifulSoup
import time, random
import mysqls
import re
from fake_useragent import UserAgent
import os

ua = UserAgent()

#设置cookies
cookie = "_lxsdk_cuid=162760423dfc8-0801f141cb0731-3b60490d-e1000-162760423dfc8; _lxsdk=162760423dfc8-0801f141cb0731-3b60490d-e1000-162760423dfc8; _hc.v=af7219c3-2b99-8bb8-f9b2-7b1d9be7f29e.1522398406; s_ViewType=10; ua=%E4%BB%A4%E7%8B%90%E5%86%B2; ctu=029e953356caf94d20233d299a70d285a03cb64585c371690b17d3e59c4c075c; cye=guangzhou; Hm_lvt_e6f449471d3527d58c46e24efb4c343e=1531964746; cy=4; dper=8c6ae023e893759ea57ce154028f1800be56b69450806b893b9cf5c6b6c3e3ba3c986c9a603bcbf9a7fb18dcd2038cf704b3e3baba3532bc7dffec965fe5e6c3b2479ca21c6577a1f5636088acbba8936df6ac994e02a923a907907a938559f9; ll=7fd06e815b796be3df069dec7836c3df; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; _lxsdk_s=1661889a264-50e-66f-22a%7C%7C276"

#修改请求头
headers = {
        'User-Agent':ua.random,
        'Cookie':cookie,
        'Connection':'keep-alive',
        'Host':'www.dianping.com',
        'Referer': 'http://www.dianping.com/shop/521698/review_all/p6'
}

#从ip代理池中随机获取ip
#ips = open('proxies.txt','r').read().split('\n')
#
#def get_random_ip():
#    ip = random.choice(ips)
#    pxs = {ip.split(':')[0]:ip}
#    return pxs

#获取html页面
def getHTMLText(url,code="utf-8"):
    try:
        time.sleep(random.random()*6 + 2)
        r=requests.get(url, timeout = 5, headers=headers, 
#                       proxies=get_random_ip()
                       )
        r.raise_for_status()
        r.encoding = code
        return r.text
    except:
        print("产生异常")
        return "产生异常"

#因为评论中带有emoji表情，是4个字符长度的，mysql数据库不支持4个字符长度，因此要进行过滤
def remove_emoji(text):
    try:
        highpoints = re.compile(u'[\U00010000-\U0010ffff]')
    except re.error:
        highpoints = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')
    return highpoints.sub(u'',text)

#从html中提起所需字段信息
def parsePage(html,shpoID):
    infoList = [] #用于存储提取后的信息，列表的每一项都是一个字典
    soup = BeautifulSoup(html, "html.parser")
    
    for item in soup('div','main-review'):
        cus_id = item.find('a','name').text.strip()
        comment_time = item.find('span','time').text.strip()
        try:
            comment_star = item.find('span',re.compile('sml-rank-stars')).get('class')[1]
        except:
            comment_star = 'NAN'
        cus_comment = item.find('div',"review-words").text.strip()
        scores = str(item.find('span','score'))
        try:
            kouwei = re.findall(r'口味：([\u4e00-\u9fa5]*)',scores)[0]
            huanjing = re.findall(r'环境：([\u4e00-\u9fa5]*)',scores)[0]
            fuwu = re.findall(r'服务：([\u4e00-\u9fa5]*)',scores)[0]
        except:
            kouwei = huanjing = fuwu = '无'
        
        infoList.append({'cus_id':cus_id,
                         'comment_time':comment_time,
                         'comment_star':comment_star,
                         'cus_comment':remove_emoji(cus_comment),
                         'kouwei':kouwei,
                         'huanjing':huanjing,
                         'fuwu':fuwu,
                         'shopID':shpoID})
    return infoList

#构造每一页的url，并且对爬取的信息进行存储
def getCommentinfo(shop_url, shpoID, page_begin, page_end):
    for i in range(page_begin, page_end):
        try:
            url = shop_url + 'p' + str(i)
            html = getHTMLText(url)
            infoList = parsePage(html,shpoID)
            print('成功爬取第{}页数据,有评论{}条'.format(i,len(infoList)))
            for info in infoList:
                mysqls.save_data(info)
            #断点续传中的断点
            if (html != "产生异常") and (len(infoList) != 0):
                with open('xuchuan.txt','a') as file:
                    duandian = str(i)+'\n'
                    file.write(duandian)
            else:
                print('休息60s...')
                time.sleep(60)
        except:
            print('跳过本次')
            continue
    return

def xuchuan():
    if os.path.exists('xuchuan.txt'):
        file = open('xuchuan.txt','r')
        nowpage = int(file.readlines()[-1])
        file.close()
    else:
        nowpage = 0
    return nowpage

#根据店铺id，店铺页码进行爬取
def craw_comment(shopID='521698',page = 53):
    shop_url = "http://www.dianping.com/shop/" + shopID + "/review_all/"
    #读取断点续传中的续传断点
    nowpage = xuchuan()
    getCommentinfo(shop_url, shopID, page_begin=nowpage+1, page_end=page+1)
    mysqls.close_sql()
    return

if __name__ == "__main__":
    craw_comment()
        