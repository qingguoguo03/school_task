# -*- coding: utf-8 -*-
"""
Created on Sat Nov 17 12:46:27 2018

@author: zhuweiwei
"""

# 检查所有目录是否按照所想都爬取了下来
import pandas as pd
col = 'JMR'
records = pd.read_pickle(col + 'papers_check.pkl')
paper_records = pd.read_pickle(col + 'papers_detail_records.pkl')
true_record = pd.read_pickle(col + 'papers_series.pkl')
done = pd.read_pickle(col + 'done.pkl')


# 首先保证所有年份都已经爬取
inds = set([str(i) for i in range(int(done[-1]), int(done[0])+1)])
if (set(done)-set(inds)):
    raise
if (set(inds)-set(done)):
    raise
    

# 处理true_records
# 处理paper_detail_records
# 验证二者的系列链接数量是否一致
for year in paper_records:
    if len(paper_records[year])!=len(true_record[year]):
        raise
   
    if set(paper_records[year]) - set(true_record[year]):
        raise
    if set(true_record[year]) - set(paper_records[year]):
        raise

# records记录每个系列 应该有多少篇论文数量
records_df = []
for year in records:
    for item in records[year]:
        records_df.append([year]+list(item))
records_df = pd.DataFrame(records_df, columns=['year', 'series_i', 'series', 'paper_n'])  

# 验证paper_detail_records
paper_detail_sys = []
for year in paper_records:
    for detail_part in paper_records[year]:
        for paper_name in paper_records[year][detail_part]:
            paper_detail_sys.append([year, detail_part, paper_name] + list(paper_records[year][detail_part][paper_name]))
paper_detail_sys = pd.DataFrame(paper_detail_sys)         
paper_detail_sys.columns = ['year','series','paper_name_','title','pdf_href']
if (set(done)-set(paper_detail_sys['year'])):
    raise
if (set(paper_detail_sys['year'])-set(done)):
    raise

# 检查每个系列论文数量是否一致
tmp = paper_detail_sys.groupby(['year','series'])[['title']].count().reset_index()
tmp = tmp.merge(records_df, how='left', on=['year','series'])
if (tmp['title'] != tmp['paper_n']).any():
    raise
    
pd.to_pickle(paper_detail_sys, '%s_download_papers.pkl' % col)
# 到此默认所有系列都爬取完全
