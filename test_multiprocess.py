# -*- coding: utf-8 -*-
"""
Created on Wed Nov  1 12:43:16 2017

@author: Administrator
"""


from multiprocessing import Pool

# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 12:16:39 2017

@author: Administrator
"""

#取出数据
from multiprocessing import Pool
import time
from gensim import models
import pandas as pd
import jieba
import pymysql
import jieba.posseg as posseg
import pickle


class Initial_Model():
    def __init__(self):
        self.model = self.load_word2vec()
        self.kws, self.label = self.load_kws()
    def load_word2vec(self):
        print('加载model')
        model = models.KeyedVectors.load_word2vec_format(r"E:/BaiduNetdiskDownload/word2vec_c",binary=False)
        print('model加载成功')
        return model
    def load_kws(self):   
        print('加载key_central5主题')
        fpath = 'E:/BaiduNetdiskDownload/key_central5.pkl'
        fr = open(fpath, 'rb')
        key_central= pickle.load(fr)
        fr.close()
        print('key_central5加载成功!')
        #给每个关键词赋值label标号，标号从1开始
        kws = []
        label = []
        keylist = list(key_central.keys())
        #['baihuo', 'bangong', 'baojian', 'baojianping', 'bendifuwu', 'canju', 'chongwu', 'diy', 'dongman', 'fangchan', 'jiadian', 'jiafan', 'jiaju', 'jiancai', 'jiashi', 'kaquan', 'kids_wanju', 'linshi', 'meishi', 'meizhuang', 'naishi', 'nan_nv_zhuang', 'neiyi', 'nongzi', 'peishi', 'qiche', 'qiche_yongping', 'shoubiao', 'shuma_shouji', 'wujin', 'xiangbao', 'xianhua', 'xiezi', 'xihu', 'xuexi', 'yanjing', 'yingshi', 'youxi', 'yueqi', 'yunchan', 'yundong', 'zhuangxiu', 'zhubao']
        print(keylist)
        for i,key in enumerate(keylist):
            label += [i+1]*len(key_central[key])
            kws += key_central[key]
        print('kws加载成功!')
        return kws, label
    def similarity_list(self, words): 
        '''
        获取相似性度量 words为分好的词组 kws为关键词
        '''
        sim = [0.0]*len(self.kws)
        for word in words:
            try:
                s = [self.model.similarity(word,kw) for kw in self.kws] #每个词语与关键词计算相似度
                sim = [sim[i] if sim[i]>similarity else similarity for i,similarity in enumerate(s)] #取最相似的关键词
            except:
                txt = [i for i in list(jieba.cut(word)) if i != ' ']
                for te in txt:
                    try:
                        s = [self.model.similarity(te,kw) for kw in self.kws]
                        sim = [sim[i] if sim[i]>similarity else similarity for i,similarity in enumerate(s)]
                    except:
                        pass
        return sim
    def sort_dict(self, al):
        '''
        对相似性值进行排序 从大到小排序并返回index
        '''
        index = range(len(al))
        dic = zip(index,al)
        reli = [ item[0] for item in sorted(dic, key=lambda x: x[1],reverse=True)]
        return reli
    def get_label(self, sim_sort,n=10):
        '''
        获取数据的所属label 默认取相似值最大的10个，然后计算10各种出现标号最多的作为这个搜索词的label
        '''
        reli = [self.label[i] for i in sim_sort[:n]]
        cnts = [(i,reli.count(i)) for i in set(reli[:n])]
        be = [ item[0] for item in sorted(cnts, key=lambda x: x[1],reverse=True)]
        return be[0]
    #核心思想就是分词加取出产品，产品应该主要是名词
    def title_process(self, title):
        ti_words = posseg.cut(title) 
        nouns = [w.word for w in ti_words if w.flag.startswith('n') ]
        return nouns
    def get_connection(self):
        conn = pymysql.connect(host='127.0.0.1',port=3306,user='root',password='',database='keywords',charset='latin1',cursorclass=pymysql.cursors.DictCursor)
        return conn
    def get_close(self,conn, cur):
        cur.close()
        conn.close()
    def readmysql(self, conn, cur, sql):
        cur.execute("set names latin1")
        doc = pd.read_sql_query(sql, conn)
    #     cur.execute('SELECT row_id,title from product_title_category limit 5000 ')
        print(type(doc),len(doc))
    #     doc = pd.DataFrame(doc)
    #     doc = cur.fetchall()
        return doc
    def exeUpdate(self, conn, cur, sql):             #更新语句，可执行Update，Insert语句
        sta=cur.execute(sql)
        return sta

tool = Initial_Model()

def worker(i):
    global tool
    res = []
    conn = tool.get_connection()
    cur = conn.cursor()
    sql = "select row_id, title from product_title_category where title is not null limit "+str(i*100)+', 100;'
    data = tool.readmysql(conn,cur,sql)
    for index in data.index[:]:
        try:
            if data.loc[index]['title']:
                try:
                    text = data.loc[index]['title'].encode('latin1').decode('gbk')
                    txtsplits = tool.title_process(text)
                    sim = tool.similarity_list(txtsplits)
                    txt_label = -1
                    if sum(sim):
                        sim_sort = tool.sort_dict(sim)
                        txt_label = tool.get_label(sim_sort)
                        rowid = data.loc[index]['row_id']
                        sql = 'update product_title_category set label=%s where row_id=%d'%(txt_label,rowid)
                except:
                    res.append(data.loc[index])
                try:
                    tool.exeUpdate(conn, cur, sql)
                except Exception as e:
                    print(e)
        except:
            pass
    conn.commit()
    tool.get_close(conn,cur)
    return res
        

def shell():
    pool=Pool(2)
    res=pool.map(worker,range(2,4))
    pool.close()
    pool.join()
    return res

if __name__ == '__main__': 
#这里主要处理用户最后购买产品的title
#购买产品转化为label的入口
    time0 = time.time()
    res = shell()
    time1 = time.time()
    print('Results:', time1-time0)
    print(len(res))
    

        
