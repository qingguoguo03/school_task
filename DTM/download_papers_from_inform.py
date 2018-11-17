# -*- coding: utf-8 -*-
import time
import requests
from bs4 import BeautifulSoup 
from collections import defaultdict
import pandas as pd
import time
import os
import re
import urllib
from urllib import parse
import selenium.webdriver.support.ui as ui
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains  
import threading

def get_FileSize(filePath):
    '''获取文件的大小,结果保留两位小数，单位为MB'''
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

def get_file(url):
    headers = {
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'           
    }
    flag = True
    cnt = 0
    while flag:
        try: 
            file= requests.get(url, stream=True, headers = headers)
            flag = False
        except Exception as e:
            cnt += 1
            print('Exception : %s' % repr(e))
        if cnt > 3:
              break
        time.sleep(10)
    return file

def download_file(file, file_name):
    with open(file_name, 'wb') as f:
        for chunk in file.iter_content(chunk_size=3*1024):  # 1024 是一个比较随意的数，表示分几个片段传输数据。
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush() #刷新也很重要，实时保证一点点的写入。
    return file_name


def get_HTML(url):
      html = None
      while not html:
          driver = webdriver.Chrome()
          driver.get(url)
          time.sleep(10)
          html = driver.page_source
          driver.close()
      return html

def download_papers(link_records, k):
      global col
      pattern = 'Published Online:</span>(.*?)</p>'
      try:
            download = pd.read_pickle('G:/XU_TASK/DTM/'+'_'.join([col, str(k),'download']) + '.pkl')
      except:
            download = defaultdict(list)
      try:
            person_load = pd.read_pickle('G:/XU_TASK/DTM/'+'_'.join([col, str(k),'person_load']) + '.pkl')
      except:
            person_load = []
      try:
            paper_details = pd.read_pickle('G:/XU_TASK/DTM/'+'_'.join([col, str(k),'paper_details']) + '.pkl')
      except:
            paper_details = defaultdict(list)
      try:
            paper_infos = pd.read_pickle('G:/XU_TASK/DTM/'+'_'.join([col, str(k),'paper_infos']) + '.pkl')
      except:
            paper_infos = defaultdict(list)
      for link in link_records:
            href = link[-2]
            if paper_infos[href]:
                if paper_infos[href][0] == len(download[href]):
                    print('  %d:  this %s done ! paper_infos: length of pdf: %d, and downlaad length: %d' % (k, href, paper_infos[href][0], len(download[href])))
                    continue
            html = get_HTML(href)
            print(' %d 得到链接*********************' % k)
            soup = BeautifulSoup(html, 'html.parser')
            PDFs = soup.find_all('div', attrs={'class':'issue-item'})
            paper_infos[href] = [len(PDFs)]
            for i, pdf in enumerate(PDFs):
                  title = pdf.find(attrs={'class':'issue-item__title'}).text
                  matcher = re.compile(pattern)
                  publish_time = ''
                  try:
                        publish_time = matcher.search(str(pdf))[1]
                  except Exception as e:
                        print(repr(e))
                  ahref = pdf.find(attrs={'title':'PDF'}).get('href')
                  paper_infos[href].append((title, publish_time, ahref))
                  pdf_href = 'https://pubsonline.informs.org' + ahref
                  if pdf_href in download[href]:
                        print('%d %s 文章已经存在：%d  %s' %(k, href, i, title))
                        continue
                  print('%d %s 正在处理文章：%d %s' %(k, href, i, title))
                  file = get_file(pdf_href)
                  file_name = 'G:/XU_TASK/DTM/NEW_PAPERS/ISR/'+ahref.replace('/','_').replace('.','_') + '.pdf'
                  print('%d 保存: %s' % (k, file_name))
                  cnt = 0
                  while cnt <= 3:
                        try:
                              download_file(file, file_name)
                              if get_FileSize(file_name) == 0:
                                    print('%d 文件为空： %s' % (k, file_name))
                                    cnt += 1
                                    continue
                              cnt = 0
                              break 
                        except Exception as e:
                              cnt += 1
                              print('%d 下载异常: %s' % (k, repr(e)))
                              time.sleep(2)
                              file = get_file(pdf_href)
                  paper_details[file_name] = [link, title, publish_time, pdf_href]
                  pd.to_pickle(paper_details, 'G:/XU_TASK/DTM/'+'_'.join([col, str(k),'paper_details']) + '.pkl')
                  if cnt > 3:
                        person_load.append((title, pdf_href, file_name, publish_time, link))
                        download[href].append(pdf_href)
                        pd.to_pickle(download, 'G:/XU_TASK/DTM/'+'_'.join([col, str(k),'download']) + '.pkl')
                        pd.to_pickle(person_load, 'G:/XU_TASK/DTM/'+'_'.join([col, str(k),'person_load']) + '.pkl')
                  if not cnt:
                        download[href].append(pdf_href)
                        pd.to_pickle(download, 'G:/XU_TASK/DTM/'+'_'.join([col, str(k),'download']) + '.pkl')
            pd.to_pickle(paper_infos, 'G:/XU_TASK/DTM/'+'_'.join([col, str(k),'paper_infos']) + '.pkl')
      pd.to_pickle(True, 'G:/XU_TASK/DTM/all_done_%s_%d.txt' % (col, k))
      # paper_infos 保证了所有文章都被抓取下来 统计paper_infos的数量与下载下来的文章数量
      
      
      
if __name__ == '__main__':
      global col, papers_detail_records, URL
      
      col = 'ISR'
   
      papers_detail_records = pd.read_pickle('G:/XU_TASK/DTM/ISRpapers_detail_records.pkl')
      threads = []
      for i in range(0,12):
          print('线程 %d 启动' % i)
          t = threading.Thread(target=download_papers, args=(papers_detail_records[i*9:(i+1)*9], i))
          threads.append(t)
          t.start()
          time.sleep(60)
      t = threading.Thread(target=download_papers, args=(papers_detail_records[108:], 12))
      threads.append(t)
      t.start()
      for t in threads:
            t.join()
      print('The documents have been downloaded successfully !')
                    

#link_records = []
#URL = 'https://pubsonline.informs.org/toc/isre/29/2'
#html = get_HTML(URL)
#soup = BeautifulSoup(html, 'html.parser')
#links = soup.find_all('div', attrs={'class':'loi__issue'})
#for link in links:
#      a = link.find('a')
#      a_href = 'https://pubsonline.informs.org' + a.get('href')
#      serie = a.span.text
#      a_text = a.text.replace(serie, '')
#      time = link.find('span',attrs={'class':'coverDate'}).text
#      iPageRange = link.find('div', attrs={'class':'iPageRange'}).text
#      link_records.append([time, serie, a_text, a_href, iPageRange])
#pd.to_pickle(link_records, 'F:/DTM/ISRpapers_detail_records.pkl')
                  
      
#URL = 'https://pubsonline.informs.org/toc/isre/29/2'
#driver = webdriver.Chrome()
#driver.get(URL)
#ARCHIVES = driver.find_element_by_xpath('//*[@id="loi-banner"]/div/a[4]')
#ARCHIVES.click()
#year = driver.find_element_by_xpath('//*[@id="pane-50c2fbc0-2f69-41a1-8f28-ed6a67b2e3230-1con"]')
#year.click()
#links = driver.find_elements_by_xpath('div[@class="parent-item"]')
#link_records = []
#for link in links[:]: 
#    link.click()
#    titles = driver.find_elements_by_xpath('//h5[@class="issue-item__title"]')
#    PDFs = driver.find_elements_by_xpath('//a[@title="PDF"]')
#    print(len(titles))
#    break
#for i, title in enumerate(titles):
#    print('title: ', title.text)
#    print('href: ', PDFs[i].get_attribute('href'))
#
#headers = {
#      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'           
#}
#req = urllib.request.Request(url=URL,headers=headers)
#html = urllib.request.urlopen(req).read()

#      filename = 'F:/DTM/ISR/_10_1287_isre_2018_0795.pdf'
      
#https://pubsonline.informs.org/doi/pdf/10.1287/isre.2018.0795
'''
https://pubsonline.informs.org/toc/isre/29/3
https://pubsonline.informs.org/toc/isre/29/3
https://pubsonline.informs.org/toc/isre/29/2
https://pubsonline.informs.org/toc/isre/29/2
https://pubsonline.informs.org/toc/isre/29/1
https://pubsonline.informs.org/toc/isre/29/1
https://pubsonline.informs.org/toc/isre/28/4
https://pubsonline.informs.org/toc/isre/28/4
https://pubsonline.informs.org/toc/isre/28/3
https://pubsonline.informs.org/toc/isre/28/3
https://pubsonline.informs.org/toc/isre/28/2
https://pubsonline.informs.org/toc/isre/28/2
https://pubsonline.informs.org/toc/isre/28/1
https://pubsonline.informs.org/toc/isre/28/1
https://pubsonline.informs.org/toc/isre/27/4
https://pubsonline.informs.org/toc/isre/27/4
https://pubsonline.informs.org/toc/isre/27/3
https://pubsonline.informs.org/toc/isre/27/3
https://pubsonline.informs.org/toc/isre/27/2
https://pubsonline.informs.org/toc/isre/27/2
https://pubsonline.informs.org/toc/isre/27/1
https://pubsonline.informs.org/toc/isre/27/1
https://pubsonline.informs.org/toc/isre/26/4
https://pubsonline.informs.org/toc/isre/26/4
https://pubsonline.informs.org/toc/isre/26/3
https://pubsonline.informs.org/toc/isre/26/3
https://pubsonline.informs.org/toc/isre/26/2
https://pubsonline.informs.org/toc/isre/26/2
https://pubsonline.informs.org/toc/isre/26/1
https://pubsonline.informs.org/toc/isre/26/1
https://pubsonline.informs.org/toc/isre/25/4
https://pubsonline.informs.org/toc/isre/25/4
https://pubsonline.informs.org/toc/isre/25/3
https://pubsonline.informs.org/toc/isre/25/3
https://pubsonline.informs.org/toc/isre/25/2
https://pubsonline.informs.org/toc/isre/25/2
https://pubsonline.informs.org/toc/isre/25/1
https://pubsonline.informs.org/toc/isre/25/1
https://pubsonline.informs.org/toc/isre/24/4
https://pubsonline.informs.org/toc/isre/24/4
https://pubsonline.informs.org/toc/isre/24/3
https://pubsonline.informs.org/toc/isre/24/3
https://pubsonline.informs.org/toc/isre/24/2
https://pubsonline.informs.org/toc/isre/24/2
https://pubsonline.informs.org/toc/isre/24/1
https://pubsonline.informs.org/toc/isre/24/1
https://pubsonline.informs.org/toc/isre/23/4
https://pubsonline.informs.org/toc/isre/23/4
https://pubsonline.informs.org/toc/isre/23/3-part-2
https://pubsonline.informs.org/toc/isre/23/3-part-2
https://pubsonline.informs.org/toc/isre/23/3-part-1
https://pubsonline.informs.org/toc/isre/23/3-part-1
https://pubsonline.informs.org/toc/isre/23/2
https://pubsonline.informs.org/toc/isre/23/2
https://pubsonline.informs.org/toc/isre/23/1
https://pubsonline.informs.org/toc/isre/23/1
https://pubsonline.informs.org/toc/isre/22/4
https://pubsonline.informs.org/toc/isre/22/4
https://pubsonline.informs.org/toc/isre/22/3
https://pubsonline.informs.org/toc/isre/22/3
https://pubsonline.informs.org/toc/isre/22/2
https://pubsonline.informs.org/toc/isre/22/2
https://pubsonline.informs.org/toc/isre/22/1
https://pubsonline.informs.org/toc/isre/22/1
https://pubsonline.informs.org/toc/isre/21/4
https://pubsonline.informs.org/toc/isre/21/4
https://pubsonline.informs.org/toc/isre/21/3
https://pubsonline.informs.org/toc/isre/21/3
https://pubsonline.informs.org/toc/isre/21/2
https://pubsonline.informs.org/toc/isre/21/2
https://pubsonline.informs.org/toc/isre/21/1
https://pubsonline.informs.org/toc/isre/21/1
https://pubsonline.informs.org/toc/isre/20/4
https://pubsonline.informs.org/toc/isre/20/4
https://pubsonline.informs.org/toc/isre/20/3
https://pubsonline.informs.org/toc/isre/20/3
https://pubsonline.informs.org/toc/isre/20/2
https://pubsonline.informs.org/toc/isre/20/2
https://pubsonline.informs.org/toc/isre/20/1
https://pubsonline.informs.org/toc/isre/20/1
https://pubsonline.informs.org/toc/isre/19/4
https://pubsonline.informs.org/toc/isre/19/4
https://pubsonline.informs.org/toc/isre/19/3
https://pubsonline.informs.org/toc/isre/19/3
https://pubsonline.informs.org/toc/isre/19/2
https://pubsonline.informs.org/toc/isre/19/2
https://pubsonline.informs.org/toc/isre/19/1
https://pubsonline.informs.org/toc/isre/19/1
https://pubsonline.informs.org/toc/isre/18/4
https://pubsonline.informs.org/toc/isre/18/4
https://pubsonline.informs.org/toc/isre/18/3
https://pubsonline.informs.org/toc/isre/18/3
https://pubsonline.informs.org/toc/isre/18/2
https://pubsonline.informs.org/toc/isre/18/2
https://pubsonline.informs.org/toc/isre/18/1
https://pubsonline.informs.org/toc/isre/18/1
https://pubsonline.informs.org/toc/isre/17/4
https://pubsonline.informs.org/toc/isre/17/4
https://pubsonline.informs.org/toc/isre/17/3
https://pubsonline.informs.org/toc/isre/17/3
https://pubsonline.informs.org/toc/isre/17/2
https://pubsonline.informs.org/toc/isre/17/2
https://pubsonline.informs.org/toc/isre/17/1
https://pubsonline.informs.org/toc/isre/17/1
https://pubsonline.informs.org/toc/isre/16/4
https://pubsonline.informs.org/toc/isre/16/4
https://pubsonline.informs.org/toc/isre/16/3
https://pubsonline.informs.org/toc/isre/16/3
https://pubsonline.informs.org/toc/isre/16/2
https://pubsonline.informs.org/toc/isre/16/2
https://pubsonline.informs.org/toc/isre/16/1
https://pubsonline.informs.org/toc/isre/16/1
https://pubsonline.informs.org/toc/isre/15/4
https://pubsonline.informs.org/toc/isre/15/4
https://pubsonline.informs.org/toc/isre/15/3
https://pubsonline.informs.org/toc/isre/15/3
https://pubsonline.informs.org/toc/isre/15/2
https://pubsonline.informs.org/toc/isre/15/2
https://pubsonline.informs.org/toc/isre/15/1
https://pubsonline.informs.org/toc/isre/15/1
https://pubsonline.informs.org/toc/isre/14/4
https://pubsonline.informs.org/toc/isre/14/4
https://pubsonline.informs.org/toc/isre/14/3
https://pubsonline.informs.org/toc/isre/14/3
https://pubsonline.informs.org/toc/isre/14/2
https://pubsonline.informs.org/toc/isre/14/2
https://pubsonline.informs.org/toc/isre/14/1
https://pubsonline.informs.org/toc/isre/14/1
https://pubsonline.informs.org/toc/isre/13/4
https://pubsonline.informs.org/toc/isre/13/4
https://pubsonline.informs.org/toc/isre/13/3
https://pubsonline.informs.org/toc/isre/13/3
https://pubsonline.informs.org/toc/isre/13/2
https://pubsonline.informs.org/toc/isre/13/2
https://pubsonline.informs.org/toc/isre/13/1
https://pubsonline.informs.org/toc/isre/13/1
https://pubsonline.informs.org/toc/isre/12/4
https://pubsonline.informs.org/toc/isre/12/4
https://pubsonline.informs.org/toc/isre/12/3
https://pubsonline.informs.org/toc/isre/12/3
https://pubsonline.informs.org/toc/isre/12/2
https://pubsonline.informs.org/toc/isre/12/2
https://pubsonline.informs.org/toc/isre/12/1
https://pubsonline.informs.org/toc/isre/12/1
https://pubsonline.informs.org/toc/isre/11/4
https://pubsonline.informs.org/toc/isre/11/4
https://pubsonline.informs.org/toc/isre/11/3
https://pubsonline.informs.org/toc/isre/11/3
https://pubsonline.informs.org/toc/isre/11/2
https://pubsonline.informs.org/toc/isre/11/2
https://pubsonline.informs.org/toc/isre/11/1
https://pubsonline.informs.org/toc/isre/11/1
https://pubsonline.informs.org/toc/isre/10/4
https://pubsonline.informs.org/toc/isre/10/4
https://pubsonline.informs.org/toc/isre/10/3
https://pubsonline.informs.org/toc/isre/10/3
https://pubsonline.informs.org/toc/isre/10/2
https://pubsonline.informs.org/toc/isre/10/2
https://pubsonline.informs.org/toc/isre/10/1
https://pubsonline.informs.org/toc/isre/10/1
https://pubsonline.informs.org/toc/isre/9/4
https://pubsonline.informs.org/toc/isre/9/4
https://pubsonline.informs.org/toc/isre/9/3
https://pubsonline.informs.org/toc/isre/9/3
https://pubsonline.informs.org/toc/isre/9/2
https://pubsonline.informs.org/toc/isre/9/2
https://pubsonline.informs.org/toc/isre/9/1
https://pubsonline.informs.org/toc/isre/9/1
https://pubsonline.informs.org/toc/isre/8/4
https://pubsonline.informs.org/toc/isre/8/4
https://pubsonline.informs.org/toc/isre/8/3
https://pubsonline.informs.org/toc/isre/8/3
https://pubsonline.informs.org/toc/isre/8/2
https://pubsonline.informs.org/toc/isre/8/2
https://pubsonline.informs.org/toc/isre/8/1
https://pubsonline.informs.org/toc/isre/8/1
https://pubsonline.informs.org/toc/isre/7/4
https://pubsonline.informs.org/toc/isre/7/4
https://pubsonline.informs.org/toc/isre/7/3
https://pubsonline.informs.org/toc/isre/7/3
https://pubsonline.informs.org/toc/isre/7/2
https://pubsonline.informs.org/toc/isre/7/2
https://pubsonline.informs.org/toc/isre/7/1
https://pubsonline.informs.org/toc/isre/7/1
https://pubsonline.informs.org/toc/isre/6/4
https://pubsonline.informs.org/toc/isre/6/4
https://pubsonline.informs.org/toc/isre/6/3
https://pubsonline.informs.org/toc/isre/6/3
https://pubsonline.informs.org/toc/isre/6/2
https://pubsonline.informs.org/toc/isre/6/2
https://pubsonline.informs.org/toc/isre/6/1
https://pubsonline.informs.org/toc/isre/6/1
https://pubsonline.informs.org/toc/isre/5/4
https://pubsonline.informs.org/toc/isre/5/4
https://pubsonline.informs.org/toc/isre/5/3
https://pubsonline.informs.org/toc/isre/5/3
https://pubsonline.informs.org/toc/isre/5/2
https://pubsonline.informs.org/toc/isre/5/2
https://pubsonline.informs.org/toc/isre/5/1
https://pubsonline.informs.org/toc/isre/5/1
https://pubsonline.informs.org/toc/isre/4/4
https://pubsonline.informs.org/toc/isre/4/4
https://pubsonline.informs.org/toc/isre/4/3
https://pubsonline.informs.org/toc/isre/4/3
https://pubsonline.informs.org/toc/isre/4/2
https://pubsonline.informs.org/toc/isre/4/2
https://pubsonline.informs.org/toc/isre/4/1
https://pubsonline.informs.org/toc/isre/4/1
https://pubsonline.informs.org/toc/isre/3/4
https://pubsonline.informs.org/toc/isre/3/4
https://pubsonline.informs.org/toc/isre/3/3
https://pubsonline.informs.org/toc/isre/3/3
https://pubsonline.informs.org/toc/isre/3/2
https://pubsonline.informs.org/toc/isre/3/2
https://pubsonline.informs.org/toc/isre/3/1
https://pubsonline.informs.org/toc/isre/3/1
https://pubsonline.informs.org/toc/isre/2/4
https://pubsonline.informs.org/toc/isre/2/4
https://pubsonline.informs.org/toc/isre/2/3
https://pubsonline.informs.org/toc/isre/2/3
https://pubsonline.informs.org/toc/isre/2/2
https://pubsonline.informs.org/toc/isre/2/2
https://pubsonline.informs.org/toc/isre/2/1
https://pubsonline.informs.org/toc/isre/2/1
https://pubsonline.informs.org/toc/isre/1/4
https://pubsonline.informs.org/toc/isre/1/4
https://pubsonline.informs.org/toc/isre/1/3
https://pubsonline.informs.org/toc/isre/1/3
https://pubsonline.informs.org/toc/isre/1/2
https://pubsonline.informs.org/toc/isre/1/2
https://pubsonline.informs.org/toc/isre/1/1
https://pubsonline.informs.org/toc/isre/1/1
'''
