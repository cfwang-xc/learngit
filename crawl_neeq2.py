# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 11:27:09 2019

@author: asus
"""

import os
import time
import datetime
import re
import requests
import random
import xlsxwriter
from lxml import html
from concurrent.futures import ThreadPoolExecutor
from functools import partial

# crawl user_agent
def crawl_ua(which=('windows',)):
    URL = 'https://developers.whatismybrowser.com/useragents/explore/operating_system_name/{plat}/{page}'
    ua_data = []
    for plat in which:
        page = 1
        while 1:
            url = URL.format(plat=plat, page=page)
            r = requests.get(url)
            print(f'抓取UA网页: {url}')
            root = html.fromstring(r.content)
            rs = root.xpath('//td[@class="useragent"]/a/text()')
            ua_data.extend(rs)
            if len(rs) < 50:
                break
            page += 1
    return ua_data

#crawl proxy_ip
def crawl_proxy(pages=30):
    URL = 'https://www.kuaidaili.com/free/inha/{page}/'
    page = 1
    ip_data = []
    while page <= pages:
        url = URL.format(page=page)
        r = requests.get(url)
        print(f'抓取代理IP网页:{url}')
        proxy_data = re.findall('"IP">(?P<ip>[^<>]+)<.*?"PORT">(?P<port>\d+)', str(r.content), re.S)
        for ip, port in proxy_data:
            proxy_ip = 'http://' + ip + ':' + port
            ip_data.append(proxy_ip)
        if not proxy_data:
            break
        page += 1
        #反爬时效大概为1秒
        time.sleep(1)
    return ip_data

def check_ip():
    '''请求百度，状态码无误，保留ip'''
    new_ips = []
    for each in crawl_proxy():
        r = requests.get('https://www.baidu.com/', timeout=3)
        if r.status_code == 200:
            new_ips.append(each)
    print('代理IP检测完毕')
    return new_ips

'''两个save，将代理池和UA池保留，避免每次都重新获取'''        
def save_ua(filename='ua_data.txt', func=crawl_ua):
    filedir = os.getcwd() + os.sep + 'packages'
    file = filedir + os.sep + filename
    downloadtime = time.time()
    if os.path.exists(filedir):
        pass
    else:
        os.mkdir(filedir)
    with open(r'{file}'.format(file=file), 'w') as f:
        f.writelines(f'{value}\n' for value in func())
        f.write(f'[logging:{downloadtime}]')

def save_ip():
    save_ua(filename='ip_data.txt', func=check_ip)

def get_ua(filename='ua_data.txt'):
    '''同一个ua间隔一秒使用'''
    file = os.getcwd() + os.sep + 'packages' + os.sep + filename
    while 1:
        try:
            f = open(file)
        except IOError:
            print(f'无{filename}数据包，抓取数据包')
            if filename == 'ua_data.txt':
                save_ua()
            elif filename == 'ip_data.txt':
                save_ip()
        else:
            break
    
    data_info = {}.fromkeys((i[:-1] for i in f if not i.startswith('[logging:')), 0)
    while 1:
        try:
            one = random.choice(list(data_info.keys()))
        except IndexError:
            if filename == 'ua_data.txt':
                save_ua()
            elif filename == 'ip_data.txt':
                save_ip()
        else:
            if time.time()-data_info[one] > 2:
                break
    return one

def get_ip():
    get_ua(filename='ip_data.txt')

def get_lasttime():
    '''UA和IP文件最后存储了一个下载时的秒数，获取秒数，对比当下超过一定时间，则更新两个文件'''
    file = os.getcwd() + os.sep + 'packages' + os.sep + 'ua_data.txt'
    while 1:
        try:
            f = open(file)
        except IOError:
            print(f'无数据包，抓取数据包')
            save_ua()
        else:
            break
    
    time_info = [i for i in f if i.startswith('[logging:')]
    last_time = float(time_info[0].replace('[logging:','').replace(']',''))
    return last_time    
    
def download(url) :
    headers = {'user-agent': get_ua()}
    proxies = {'http': get_ip()}
    r = requests.get(url, headers=headers, proxies=proxies)
    return r.content.decode('utf-8')
   
def witer_sheet(name='title_data'):
    xlsxname = name + '.xlsx'
    workbook = xlsxwriter.Workbook(xlsxname)
    worksheet = workbook.add_worksheet('title_data')
    worksheet.write_row('A1',['code', 'shortname', 'title', 'pubdate'] )
    worksheet.set_column('B:B', 10)
    worksheet.set_column('C:C', 70)
    row = 2
    for each in neeq_data:
        worksheet.write_row(f'A{row}',each)
        row += 1
    else:
        workbook.close()
    print('数据写入完毕。文件路径：', os.getcwd() + xlsxname)
    time.sleep(2)
    

def page():
    return  all_pages
all_pages = 1  #所有页面
neeq_data = [] #爬虫数据结果
lasttime = get_lasttime() if get_lasttime() else 0
def crawl_neeq(start='2019-06-11', kind='5', retry=10, page=0):
    global all_pages
    title_info = []
    URL = 'http://www.neeq.com.cn/disclosureInfoController/infoResult.do?disclosureType={kind}&page={page}&companyCd=&startTime={start}&endTime=&keyword='
    url = URL.format(start=start, page=page, kind=kind)
    try:
        html = download(url)
        print(f'抓取页面:{url}')
    except Exception as e:
        print('Exception: %s %s' % (e, url))
        if retry > 0:
            print(f'重新抓取:{url}')
            download(url)
            retry -= 1
    else:
        title_data = re.findall(r'"companyCd".*?(?P<code>\d{6}).*?"companyName"[^"]"(?P<shortname>[^"]+).*?"disclosureTitle"[^"]"(?P<title>.*?)","disclosureType.*?publishDate.*?(?P<pubdate>\d{4}-\d{2}-\d{2})', str(html), re.S)
        page = re.findall('totalPages":(\d+)', str(html), re.S)
        try:
            page = page[0]
        except IndexError:
            page = 0

        if all_pages < int(page):
            all_pages = int(page)
        else:
            all_pages = 1
            
        if title_data:
            for code, shortname, title, pubdate in title_data:
                title_info = [code, shortname, title, pubdate]
                #print(title_info)
                neeq_data.append(title_info)
    time.sleep(2)

def crawl_thread(starttime, kind, retry):
    new_crawl = partial(partial(partial(crawl_neeq, starttime), kind), retry)
    return new_crawl

def main():
    global all_pages
    now = time.time()
    if now - lasttime > 864000:
        print('时间过了十天，更新代理数据包')
        save_ua()
        save_ip()
    
    print('-' * 40)
    while 1:
        starttime = input('输入开始日期[yyyy-mm-dd]-->')
        if re.match('\d{4}-\d{2}-\d{2}', starttime):
            break
        else:
            print('日期有误，重新输入')
    if not starttime:
        if datetime.datetime.now().weekday() == 0:  # Monday
            starttime = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
        else:
            starttime = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    #分开抓取，避免其他页面公告对all_pages产生影响
    #抓取第5页面
    start_crawl = crawl_thread(starttime, 5, 10)
    start_crawl()
    with ThreadPoolExecutor(max_workers = 30) as executor:
        executor.map(start_crawl, range(2, all_pages+1))
    
    #抓取第8页面
    all_pages = 1
    start_crawl = crawl_thread(starttime, 8, 10)
    start_crawl()
    with ThreadPoolExecutor(max_workers = 20) as executor:
        executor.map(start_crawl, range(2, all_pages+1))
   
    #抓取第6页面
    all_pages = 1
    start_crawl = crawl_thread(starttime, 6, 10)
    start_crawl()
    with ThreadPoolExecutor(max_workers = 20) as executor:
        executor.map(start_crawl, range(2, all_pages+1))
    
    #抓取第9页面
    all_pages = 1
    start_crawl = crawl_thread(starttime, 9, 10)
    start_crawl()
    with ThreadPoolExecutor(max_workers = 20) as executor:
        executor.map(start_crawl, range(2, all_pages+1))
    
    witer_sheet(name=starttime)

if __name__ == '__main__':
    main()
        