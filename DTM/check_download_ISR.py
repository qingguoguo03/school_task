# -*- coding: utf-8 -*-
"""
Created on Sun Sep 16 11:16:40 2018

@author: zhuweiwei
"""
import time
import requests
from bs4 import BeautifulSoup 
from collections import defaultdict
import pandas as pd
import time
import os
import re
import json
from urllib import parse
import selenium.webdriver.support.ui as ui
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains  
import threading
from PyPDF2 import PdfFileReader
from PyPDF2 import PdfFileWriter
from pdfminer.pdfparser import PDFParser,PDFDocument

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

def get_reader(filename):
    try:
        fp = open(filename, 'rb')
        praser = PDFParser(fp)
        doc = PDFDocument()
        # 连接分析器 与文档对象
        praser.set_document(doc)
        doc.set_parser(praser)
        fp.close()
        print('open file successfully')
        return (True, 'open')
    except Exception as err:
        if 'stream with no endstream' in str(err):
            return (False, repr(err)) # 不完整引起
        else:
            return (None, repr(err)) # 'PSEOF()' 文件大小为0引起
        
def download_papers(i, k):
    global col, reload, path

#    try:
#        download = pd.read_pickle(path + '_'.join([col, str(k),'download']) + '.pkl')
#    except:
#        download = []
    try:
        person_load = pd.read_pickle(path + '_'.join([col, str(k),'person_load']) + '.pkl')
    except:
        person_load = []
    try:
        html = pd.read_pickle(path + '_'.join([col, str(k),'html']) + '.pkl')
    except:
        html = defaultdict(dict)
     
    for ind in reload.index: # reload.iloc[i:(i+1)*35].index
        time.sleep(3)
        paper_path, pdf_href, title = reload.loc[ind, ['paper_path','pdf_href', 'title']]
        if (paper_path in html) and html[paper_path]['flag']:
            print('%d %s 文章已经存在' %(k, pdf_href))
            continue
        print('%d %s 正在处理文章' %(k, pdf_href))
        file = get_file(pdf_href)
        if title not in file.text:
            flag = False
            time.sleep(60)
        else:
            flag = True
        html[paper_path] = {
            'pdf_href':pdf_href,
            'headers': file.headers['Content-Type'],
            'flag': flag
        }
        if 'pdf' not in file.headers['Content-Type']:
            pd.to_pickle(html, path + '_'.join([col, str(k),'html']) + '.pkl')
            continue
        cnt = 0
        while cnt <= 5:
            try:
                download_file(file, paper_path)
                if get_FileSize(paper_path) == 0:
                    print('%d 文件为空： %s' % (k, paper_path))
                    cnt += 1
                    continue
                else:
                    flag, error_msg = get_reader(paper_path)
                    if flag:
                        cnt = 0
                        break 
                    else:
                        raise Exception(error_msg)
            except Exception as e:
                cnt += 1
                print('%d 下载异常: %s' % (k, repr(e)))
                time.sleep(2)
                file = get_file(pdf_href)
        if cnt > 3:
            person_load.append((paper_path, pdf_href))
            pd.to_pickle(person_load, path + '_'.join([col, str(k),'person_load']) + '.pkl')
        pd.to_pickle(html, path + '_'.join([col, str(k),'html']) + '.pkl')
     
    pd.to_pickle(html, path + '_'.join([col, str(k),'html']) + '.pkl')
    pd.to_pickle(True, path + 'all_done_%s_%d.txt' % (col, k))
                  
if __name__ == '__main__':
      global col, reload, path
      
      path = 'F:/DTM/reload_ISR/'
      col = 'ISR'
      reload = pd.read_pickle('F:/DTM/reload_ISR/ISR_paper_reload.pkl')
      reload['paper_path_'] = reload['paper_path']
      reload['paper_path'] = path + reload['paper_name']
      threads = []
#      for i in range(0, 10):
      
      i = 0
      k = i+1
      print('线程 %d 启动' % k)
      download_papers(i, k)
#      t = threading.Thread(target=download_papers, args=(i, k))
#      threads.append(t)
#      t.start()
#      time.sleep(30)
#      for t in threads:
#          t.join()
      print('The documents have been downloaded successfully !')