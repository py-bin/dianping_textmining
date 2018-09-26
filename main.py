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
cookie = "_lxsdk_cuid=162760423dfc8-0801f141cb0731-3b60490d-e1000-162760423dfc8; _lxsdk=162760423dfc8-0801f141cb0731-3b60490d-e1000-162760423dfc8; _hc.v=af7219c3-2b99-8bb8-f9b2-7b1d9be7f29e.1522398406; s_ViewType=10; ua=%E4%BB%A4%E7%8B%90%E5%86%B2; ctu=029e953356caf94d20233d299a70d285a03cb64585c371690b17d3e59c4c075c; cye=guangzhou; Hm_lvt_e6f449471d3527d58c46e24efb4c343e=1531964746; cy=4; dper=8c6ae023e893759ea57ce154028f180070cc7d1c04b6b70eba95f5d35b1d8ddd82e11aa51441187a6431063dfe2cd7b4fb2dd1eb4d13d9a61381de2fbaac2d10fb88310ef5ae6504f5bf44395249a1c8c85a2b14e06b3ed82b6849e225e5b6a3; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; ll=7fd06e815b796be3df069dec7836c3df; _lxsdk_s=166137f187f-0b6-191-c14%7C%7C68"

#修改请求头
headers = {
        'User-Agent':ua.random,
        'Cookie':cookie,
        'Connection':'keep-alive',
        'Host':'www.dianping.com',
}

#获取html页面
def getHTMLText(url,code="utf-8"):
    try:
        time.sleep(random.random()*6 + 5)
        r=requests.get(url, timeout = 5, headers=headers)
        r.raise_for_status()
        r.encoding = code
        return r.text
    except:
        print("产生异常")
        time.sleep(60)
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
        comment_star = item.find('span',re.compile('sml-rank-stars')).get('class')[1]
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
            with open('xuchuan.txt','a') as file:
                duandian = str(i)+'\n'
                file.write(duandian)
        except:
            continue
    return

#根据店铺id，店铺页码进行爬取
def craw_comment(shopID='518986',page = 699):
    shop_url = "http://www.dianping.com/shop/" + shopID + "/review_all/"
    #断点续传中的续传
    if os.path.exists('xuchuan.txt'):
        file = open('xuchuan.txt','r')
        nowpage = int(file.readlines()[-1])
        file.close()
    else:
        nowpage = 0
    
    getCommentinfo(shop_url, shopID, page_begin=nowpage+1, page_end=page+1)
    mysqls.close_sql()
    return

if __name__ == "__main__":
    craw_comment()
        