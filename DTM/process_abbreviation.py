
from nltk.corpus import stopwords, wordnet
import re, string
import pandas as pd
from textrank import TextRank
from nltk import word_tokenize, pos_tag
from nltk.stem import WordNetLemmatizer
from collections import defaultdict
from nltk.tokenize import MWETokenizer
import itertools

pattern_upper = re.compile('^[A-Z2]{2,}$')
pattern_url = re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
pattern_suox = re.compile('\(([A-Za-z]+)\)')
stoplist = stopwords.words('english')
wnl = WordNetLemmatizer()
def lemmatize(sentence):
    ''' 先词性判断 然后词性标注 最后还原'''
    sentence = sentence.strip()
    sentence = pattern_url.sub('', sentence) # 替换链接
    # 查找缩写词的话 进行不对原文有变化
#    for c in string.digits: #去数字
#        sentence = sentence.replace(c, '')
    sentence = sentence.replace('/', ' / ') # 文章中有时候会包含x/v类似于这样 所以将这种进行分离
    tokens1 = sentence.split()  # 分词
    # 标点符号保留 然后括号也保留 其余粘连在单词上的异常符号都去除
    tokens = []
    for token in tokens1:
        if (not token):
            continue
        if len(token)>1:
            if (not token[0].isalpha()) and token[0] != '(':
                token = token[1:] # 去掉奇怪的符号
            if (not token[-1].isalpha()) and token[-1] != ')':
                token = token[:-1]
        if token:
            tokens.append(token)
    tokens1 = tokens.copy()
    # 对token进行小写: 只对首字母大写 进行小写
    tokens = [token.lower() if (token.istitle() and (not pattern_upper.search(token))) or token.islower() else token for token in tokens1]
    # 对所有词进行名词还原 因为有时候缩写词不一样 是因为复数的原因
    lemmas_sent = []
    for token in tokens:
        word_lemmas = token
        if pos_tag([token])[0][1][0] == 'N':  # 只还原名词,必须为小写才成立
            word_lemmas = wnl.lemmatize(token, pos=wordnet.NOUN)
        lemmas_sent.append(word_lemmas) # 词形还原
    
#    ans = []
#    for i, token in enumerate(tokens):
#        if tokens[i] != lemmas_sent[i]:
#            ans.append(token)
    return lemmas_sent


data = pd.read_pickle('data.pkl') # 读取摘要
data.index = range(len(data))

# 获取可能的缩写词(MMCC)
phrases = defaultdict(list)
doc_phrases = defaultdict(dict)
error = []
for k in range(len(data)):
    text = data['abstract'][k]
    # 做个词性还原: 目的是把复数名词还原为单数
    tokens = lemmatize(text)
    for i, token in enumerate(tokens):
        token = token.strip()
        try: # 专门匹配缩写词
            suox = pattern_suox.search(token).group(1)
            # 存在缩写则进行修正 将短语用连字符号进行连接
            capitals = [ c for c in list(suox) if c.isupper()] # 收集大写字母部分
            if not capitals: # 不存在大写字母 那么认为这个不是缩写词
                continue
            cnt = len(capitals) + 3 # 强调向前遍历的单词数量
            j = i # 遍历必须从后往前实现
            tmp = []
            while cnt>0 and capitals: # 遍历有剩余 且 仍然还有大写字母单词没有遍历完全
                j -= 1
                cnt -= 1
                if tokens[j] in stoplist: # 强调缩写词中包含了停用词
                    if tmp and capitals and capitals[-1].isupper() and tokens[j][0].islower():
                        tmp.append(tokens[j])
                        continue
                if '-' in tokens[j]: # 连字符号构成的缩写词
                    flag = False
                    for c in [word[0] for word in tokens[j].split('-')][::-1]:
                        if c.lower() == capitals[-1].lower():
                            capitals = capitals[:-1]
                            flag = True
                    if flag:
                        tmp.append(tokens[j])
                elif tokens[j][0].lower() == capitals[-1].lower(): # 最普通的情况
                    tmp.append(tokens[j])
                    capitals = capitals[:-1]
                elif tmp and capitals: # 还没有完全找完缩写词 而这个token以上三种都不符合 
                    tmp.append(tokens[j])
            if tmp and cnt != 0: # 说明大写的词语都找完了
                phrases[suox].append([k, suox, str(tuple(tmp[::-1]))])
                doc_phrases[k][str(len(doc_phrases[k])) + '_' + suox] = ' '+' '.join([item for item in tmp[::-1]]) + ' '
            else:
                error.append((i, text, suox)) # 没有找到的
        except:
            pass
        
