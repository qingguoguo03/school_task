# -*- coding: utf-8 -*-
"""
Created on Thu Nov  1 21:23:51 2018

@author: zhuweiwei
"""
import time
import requests
from collections import defaultdict
import pandas as pd
import os
import re
import urllib
from urllib import parse
from selenium import webdriver
import threading

def get_FileSize(filePath):
    '''获取文件的大小,结果保留两位小数，单位为MB'''
#    filePath = unicode(filePath,'utf8')
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024*1024)
    return round(fsize,2)

def mkdir(path):
    import os
    path=path.strip() # 去除首位空格
    path=path.rstrip("/") # 去除尾部 \ 符号
    isExists=os.path.exists(path)
    if not isExists:
        # 如果不存在则创建目录
        os.makedirs(path)

def get_new_token(url=''):
    global URL
    # 第一步获取新的token
    token = None
    while not token:
        try:
            driver = webdriver.Chrome()
            driver.get(url)
            time.sleep(2)
            year_link = driver.find_element_by_xpath('//*[@id="VolumeTable"]/tbody/tr/td/a[@class="medium-normal"]')
            while '+' in year_link.text:
                year_link.click()
                time.sleep(2)
                year_link = driver.find_element_by_xpath('//*[@id="VolumeTable"]/tbody/tr/td/a[@class="medium-normal"]')
            detail_link = driver.find_element_by_xpath('//*[@id="VolumeTable"]/tbody/tr/td[@class="authVolIssue_issue_cell"]')
            detail_link.find_element_by_tag_name('a').click()
            paper_link = driver.find_element_by_xpath('//*[@title="PDF Full Text"]') # 得到one pdf的链接
            paper_link.click()
            paper = driver.find_element_by_xpath('//*[@id="TOCItems"]').find_element_by_tag_name('a')
            href = paper.get_attribute('href')
            token = parse.parse_qs(parse.urlparse(href).query)['EbscoContent'][0]
            driver.close()
        except Exception as e:
            print('Get_New_TOKEN: ', repr(e))
    return token

def get_file(token, url):
    flag = True
    pattern = re.compile(r'EbscoContent=(.*?)&ContentCustomer')
    while flag:
        try: 
            url_new = re.sub(pattern, 'EbscoContent=%s&ContentCustomer' % token, url)
            file= requests.get(url_new, stream=True)
            flag = False
        except urllib.error.URLError as e:  #HTTPError: HTTP Error 404: Token expired
            if e.code == 404:
                token = get_new_token()
        except Exception as e:
            print('Exception : %s' % repr(e))
        time.sleep(5)
    return token, file

def download_file(file, file_name):
    with open(file_name, 'wb') as f:
      for chunk in file.iter_content(chunk_size=10*1024):  # 1024 是一个比较随意的数，表示分几个片段传输数据。
          if chunk: # filter out keep-alive new chunks
              f.write(chunk)
              f.flush() #刷新也很重要，实时保证一点点的写入。
    return file_name


def download_papers(years, i):
      global col, papers_detail_records
      
      token = 'dGJyMNXb4kSeprU4zOX0OLCmr1CeprNSsKm4SLGWxWXS'
      path = "F:/DTM/PAPERS/" + col
      for year in years:
            print('正在处理年份：', year)
            try:
                  download = pd.read_pickle('F:/DTM/'+'_'.join([col, str(i),'download']) + '.pkl')
            except:
                  download = defaultdict(list)
            try:
                  person_load = pd.read_pickle('F:/DTM/'+'_'.join([col, str(i),'person_load']) + '.pkl')
            except:
                  person_load = []
                  
            file_path = path + '/%s/' % year
            mkdir(file_path)
            for series in papers_detail_records[year]:
                  print('正在处理期刊 ：', series)
                  item = papers_detail_records[year][series]
                  for paper_name in item:
                        url = item[paper_name][1]
                        title = item[paper_name][0]
                        print('请求链接： %s %s' % (url, title))
                        file_name = file_path + paper_name + '.pdf'
                        if file_name in download[series]:
                              print(file_name,'  文件已经存在')
                              continue
                        print('正在处理文章：' ,title)
                        token, file = get_file(token, url)
                        print('保存: ', file_name)
                        cnt = 0
                        while cnt <= 3:
                              try:
                                    download_file(file, file_name)
                                    if get_FileSize(file_name) == 0:
                                          print('文件为空：', file_name)
                                          cnt += 1
                                          continue
                                    cnt = 0
                                    break 
                              except Exception as e:
                                    cnt += 1
                                    print('下载异常: ', repr(e))
                                    time.sleep(2)
                        if cnt > 3:
                              person_load.append((year, series, title, url, paper_name, file_name))
                              download[series].append(file_name)
                              pd.to_pickle(person_load, 'F:/DTM/'+'_'.join([col, str(i),'person_load']) + '.pkl')
                        if not cnt:
                              download[series].append(file_name)
                              pd.to_pickle(download, 'F:/DTM/'+'_'.join([col, str(i),'download']) + '.pkl')
      pd.to_pickle(True, 'F:/DTM/all_done_%s_%d.txt' % (col, i))
                  
if __name__ == '__main__':
      global col, papers_detail_records, URL
      
      URLS = {
          'MIS': 'http://web.b.ebscohost.com/bsi/command/detail?vid=4&sid=c0511c40-c011-4633-a702-b882c68a1d1d%40pdc-v-sessmgr01&bdata=JnNpdGU9YnNpLWxpdmU%3d#jid=MIS&db=bth',
          'JMR': 'http://web.b.ebscohost.com/bsi/command/detail?vid=12&sid=2d6a65a8-e053-4c28-830f-b044398440d1%40pdc-v-sessmgr05&bdata=JnNpdGU9YnNpLWxpdmU%3d#jid=MKR&db=bsu',
          'JM': 'http://web.b.ebscohost.com/bsi/command/detail?vid=17&sid=2d6a65a8-e053-4c28-830f-b044398440d1%40pdc-v-sessmgr05&bdata=JnNpdGU9YnNpLWxpdmU%3d#jid=JMK&db=bsu'
      }
      col = 'JM'
      URL = URLS[col]
      papers_detail_records = pd.read_pickle('F:/DTM/%spapers_detail_records.pkl' % col)
      links = [str(i) for i in range(1936, 2016)]  
      th1 = ['1936', '1937', '1938', '1939']
      th1.extend(links[16:24])
      th2 = ['1940', '1941', '1942', '1943']
      th2.extend(links[24:28])
#      th9 = ['1964','1965']
      th3 = ['1944', '1945', '1946', '1947']
      th3.extend(links[32:38])
#      th10 = ['1974']
#      th4 = ['1948', '1949', '1950', '1951','1996']
#      th4.extend(links[40:42])
#      th11 = ['1978','1979']
#      th12 = ['1980','1981']
#      th13 = ['1982','1983']
#      th5 = links[48:56]
#      th14 = ['1998', '1999']
#      th6 = links[56:60]
#      th7 = links[64:68]
#      th8 = links[72:80] + ['2006', '2007','1997']
#      th15 = ['2004', '2005']
      
      
#      t1 = threading.Thread(target=download_papers, args=(th1, 1))
#      t2 = threading.Thread(target=download_papers, args=(th2, 2))
#      t3 = threading.Thread(target=download_papers, args=(th3, 3))
#      t4 = threading.Thread(target=download_papers, args=(th4, 4))
#      t5 = threading.Thread(target=download_papers, args=(th5, 5))
#      t6 = threading.Thread(target=download_papers, args=(th6, 6))
#      t7 = threading.Thread(target=download_papers, args=(th7, 7))
#      t8 = threading.Thread(target=download_papers, args=(th8, 8))
      
      threads = [th1,th2,th3]#,th4,th5,th6,th7,th8,th9,th10,th11,th12,th13,th14,th15]
      for i, t in enumerate(threads):
            k = i+1
            t = threading.Thread(target=download_papers, args=(t, k))
            t.start()
      for t in threads:
            t.join()
      print('The documents have been downloaded successfully !')
    
      # download = pd.to_pickle(defaultdict(list), 'G:\\XU_TASK\\DTM\\'+col+'_download.pkl')
#      download = pd.read_pickle('F:/DTM/'+col+'_download.pkl')
#      person_load = pd.read_pickle(col + '_person_load.pkl')
      
#      token = 'dGJyMNLe80Sepq84zOX0OLCmr1CeprdSsKi4S7GWxWXS' #get_new_token()
