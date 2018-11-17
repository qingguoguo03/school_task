# ISRURL = 'http://web.b.ebscohost.com/bsi/command/detail?vid=8&sid=2d6a65a8-e053-4c28-830f-b044398440d1%40pdc-v-sessmgr05&bdata=JnNpdGU9YnNpLWxpdmU%3d#jid=1LD&db=bth'
# # ISRURL 下载不了
# JMRURL = 'http://web.b.ebscohost.com/bsi/command/detail?vid=12&sid=2d6a65a8-e053-4c28-830f-b044398440d1%40pdc-v-sessmgr05&bdata=JnNpdGU9YnNpLWxpdmU%3d#jid=MKR&db=bsu'
# url=JMRURL
# driver = webdriver.Firefox()
# driver.get(url)
# wait = ui.WebDriverWait(driver,15)

# 点击所有的列表获取到底有多少系列
import time
import urllib.request
from bs4 import BeautifulSoup 
from collections import defaultdict
import pandas as pd
import selenium.webdriver.support.ui as ui
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains 
from selenium.common.exceptions import WebDriverException
import time

global n
global cnt
global URL
cnt = 0
col = 'JMR'   
JMRURL = 'http://web.b.ebscohost.com/bsi/command/detail?vid=12&sid=2d6a65a8-e053-4c28-830f-b044398440d1%40pdc-v-sessmgr05&bdata=JnNpdGU9YnNpLWxpdmU%3d#jid=MKR&db=bsu'
URL=JMRURL
def open_driver(url=URL):
    driver = webdriver.Firefox()
    driver.get(url)
    return driver

def main():
    global n, cnt, URL
    driver=open_driver(URL)
    try:
        records = paper_records = true_record = done = None
        records = pd.read_pickle(col + 'papers_check.pkl')
        paper_records = pd.read_pickle(col + 'papers_detail_records.pkl')
        true_record = pd.read_pickle(col + 'papers_series.pkl')
        done = pd.read_pickle(col + 'done.pkl')
    except:
        if not records:
            records = defaultdict(list)
        if not paper_records:
            paper_records = defaultdict(dict)
        if not true_record:
            true_record = defaultdict(list)
        if not done:
            done = []
    while True:
        try:
            year_links = None
            while not year_links: # 获取年份列表
                time.sleep(2)
                year_links = driver.find_elements_by_xpath('//*[@id="VolumeTable"]/tbody/tr/td/a[@class="medium-normal"]')
            if len(year_links) == len(done): 
                print('所有列表已经爬取完成！')
                break
            #n = len(year_links) #记录n以免异常跳出不好进行检查n
            for i, year_link in enumerate(year_links):
                year = year_link.text[-4:]
                if year in done: # 表示该年份论文都已经处理完全了
                    continue
                if '+' in year_link.text: # 首次点击列表
                        year_link.click()
                while True:
                    time.sleep(2) # 数据库反应时间有点慢
                    year_links_tmp = driver.find_elements_by_xpath('//*[@id="VolumeTable"]/tbody/tr/td/a[@class="medium-normal"]')
                    if '--' in year_links_tmp[i].text: # 点击成功才退出 不然再点击
                        break
                    else: # 说明没有展开列表
                        year_links_tmp[i].click()
                print('正在处理年份：', year)
                b = time.time()
                # 获取列表的链接
                detail_links = driver.find_elements_by_xpath('//*[@id="VolumeTable"]/tbody/tr/td[@class="authVolIssue_issue_cell"]')
                if year not in true_record: # 收集所有的系列
                    for i, detail_link in enumerate(detail_links):
                        true_record[year].append(detail_link.text) 
                    pd.to_pickle(true_record, col + 'papers_series.pkl')
                for i, detail_link in enumerate(detail_links): # 对下面的链接进行遍历
                    detail_part = detail_link.text
                    if detail_part in paper_records[year]: # 之前保存过所以不需要继续读取
                        continue 
                    print('正在处理链接：', detail_part)
                    detail_link.find_element_by_tag_name('a').click()
                    paper_link = driver.find_element_by_xpath('//*[@title="PDF Full Text"]') # 得到one pdf的链接
                    paper_link.click() 
                    papers = driver.find_element_by_xpath('//*[@id="TOCItems"]').find_elements_by_tag_name('a')
                    n = len(papers)
                    records[year].append((i, detail_part, n))
                    pd.to_pickle(records, col + 'papers_check.pkl')
                    temp = {}
                    for j, paper in enumerate(papers):
                        href = paper.get_attribute('href')
                        title = paper.get_attribute('title')
                        temp[str(i)+'_'+str(j)] = (title, href)
                        print('正在处理文章：%d' % j, title )
                    paper_records[year][detail_part] = temp
                    pd.to_pickle(paper_records, col + 'papers_detail_records.pkl')
                    # 判断一下 系列是否去取完
                    if len(true_record[year]) == len(paper_records[year]):
                        done.append(year)
                        pd.to_pickle(done, col + 'done.pkl')
                    print('准备跳转到初始页面')
                    print('用时：', time.time()-b)
                    driver.back()
                    driver.back()
        except Exception as e:
            # 发生异常重新开启浏览器
            driver.close()
            driver = open_driver()
            records = pd.read_pickle(col + 'papers_check.pkl')
            paper_records = pd.read_pickle(col + 'papers_detail_records.pkl')
            true_record = pd.read_pickle(col + 'papers_series.pkl')
            done = pd.read_pickle(col + 'done.pkl')
            cnt += 1


while True:
    try:
        main()
    except WebDriverException as e:
        pass
    done = pd.read_pickle(col + 'done.pkl')
    if len(done) == n:
        break
