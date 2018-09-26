# 大众点评评论文本挖掘

[TOC]

##一、爬虫

### 整体思路

爬取大众点评十大热门糖水店的评论，爬取网页后从html页面中把需要的字段信息（顾客id、评论时间、评分、评论内容、口味、环境、服务、店铺ID）提取出来并存储到MYSQL数据库中。

### 网页爬取和解析

链接格式为"http://www.dianping.com/shop/" + shopID + "/review_all/" + pi，如：http://www.dianping.com/shop/518986/review_all/p1，一页评论有20条。我们使用for循环构造链接URL，使用requests库发起请求并把html页面爬取下来，通过BeautifulSoup和re库解析页面提取信息。

我们发现完整的评论都存储在'div','main-review'中，且部分页面口味、环境、服务并不是每一页都有，因此需要使用try...except...防止程序中断，BeautifulSoup部分代码如下：

``` python
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
```

### 数据存储

我们使用MYSQL数据库，安装教程参考[菜鸟教程](http://www.runoob.com/mysql/mysql-install.html)，python连接MYSQL数据推荐使用pymysql，同样是推荐菜鸟教程[菜鸟教程](http://www.runoob.com/python3/python3-mysql.html)。我们需要先建立一个数据库和表，然后连接并定义游标，然后写对应的sql语句，最后执行事务，存储部分的代码如下：

``` python
#连接MYSQL数据库
db = pymysql.connect("localhost","root","","TESTDB" )
cursor = db.cursor()
#存储爬取到的数据
def save_data(data_dict):
    sql = '''INSERT INTO DZDP(cus_id, comment_time, comment_star, cus_comment, kouwei, huanjing,           fuwu, shopID) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
    value_tup = (data_dict['cus_id']
                 ,data_dict['comment_time']
                 ,data_dict['comment_star']
                 ,data_dict['cus_comment']
                 ,data_dict['kouwei']
                 ,data_dict['huanjing']
                 ,data_dict['fuwu']
                 ,data_dict['shopID']
                 )
    try:
        cursor.execute(sql,value_tup)
        db.commit()
    except:
        print('数据库写入失败')
    return
```

### 反爬虫对抗

1. **修改请求头中浏览器信息**：使用fake_useragent第三方库，修改request中的headers参数，用法如下：

   ``` python
   from fake_useragent import UserAgent
   ua = UserAgent()
   headers = {'User-Agent':ua.random}
   ```

2. **设置跳转路径**：在访问评论时，一般的浏览行为是从某一页跳转到下一页这样的，而不是直接通过连接访问，为了更好的伪装成一个正常的访问，我们需要设置一下跳转的路径，修改headers中的Referer参数

   ``` python 
   headers = {
           'User-Agent':ua.random,
           'Cookie':cookie,
           'Referer': 'http://www.dianping.com/shop/518986/review_all'
   }
   ```

3. **设置Cookies**：评论数据需要登录后才能获取，下面介绍一种非常简单方便的绕过登录的方法。

   - 在网页上进行登录
   - 使用Chrome浏览器的开发者工具，查询当前请求的cookie
   - 复制浏览器中的cookie，使用此cookie对我们的请求进行伪装

4. **使用IP代理池**：这里使用西刺代理的免费代理，构建一个爬虫爬取西刺代理的ip，然后进行验证，筛掉不可用的ip，构建出ip池供后续调用，代码来自网络。但是经过测试，大众点评对一个账号不同ip访问监控非常严格，使用IP代理池不更换账号的话，死的更快，封你账号，然而构建账号池比较麻烦，我们先暂缓。

5. **降低爬取频率**：一个简单又有效的方法就是降低爬取频率，毕竟高频率的爬取对服务器也是一个考验，如果对速度的要求不是很高的话，建议把频率放慢一点，你好我好大家好！

   ``` python
   import random
   import time
   time.sleep(6*random.random() + 4)
   ```

6. **设置断点续传**：即使降低了爬取频率，有时还是会被美团的网络工程师抓到的，小哥哥饶命啊~。因此我们需要一个断点续传的小功能，避免每次都从头开始爬。思路是建一个文本文件，存储当前爬取的进度，每次运行程序时都出当前进度开始，详见代码~

## 二、数据分析

### 数据集探索

### 数据可视化分析

## 三、评论文本的情感分析

###数据清洗

### 中文分词

### 文本特征提取（TF-IDF）

### 机器学习建模

### 模型评估测试

## 四、拓展应用及后续方向

### 标签识别

### 优化情感分析