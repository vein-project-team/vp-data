# -*- coding: utf-8 -*-
"""
Created on Tue May 10 14:44:36 2022

@author: Lenovo
"""

import pandas as pd
import jieba
import jieba.posseg as pseg
import re


def word_slice_cn(text_org):
    corpus =[]
    corpus.append(text_org.strip())
    stripcorpus = corpus.copy()
    for i in range(len(corpus)):
        stripcorpus[i]= re.sub("@([\s\S]*?):","", corpus[i])
        stripcorpus[i]= re.sub("\[([\S\s]*?)\]","", stripcorpus[i])
        stripcorpus[i]= re.sub("@([\s\S]*?)","", stripcorpus[i])
        stripcorpus[i]= re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+","", stripcorpus[i])
        stripcorpus[i]= re.sub("[^\u4e00-\u9fa5]","", stripcorpus[i])
    onlycorpus =[]
    for string in stripcorpus:
        if(string ==''): 
            continue
        else:
            if(len(string)<5): 
                continue
            else:
                onlycorpus.append(string)
    cutcorpusiter = onlycorpus.copy()
    cutcorpus = onlycorpus.copy()
    wordtocixing =[] #储存分词后的词语
    for i in range(len(onlycorpus)):
        cutcorpusiter[i]= pseg.cut(onlycorpus[i]) #使用jieba库切割单词
        cutcorpus[i]=""
        for every in cutcorpusiter[i]:
            cutcorpus[i]=(cutcorpus[i]+" "+str(every.word)).strip()
            wordtocixing.append(every.word)
    return wordtocixing


def load_list_from_txt(filepath, encoding = 'utf-8'):
    stopwords = [line.strip() for line in open(filepath, 'r', encoding=encoding).readlines()]
    return stopwords

def generate_summary(text_org, sentence_num=1):
    stopwords = load_list_from_txt("analysis/stopwords_cn.txt")
    #摘取文本中所有单词    
    text_words = word_slice_cn(text_org)
    #统计词频
    word_count = {}  
    for word in text_words: #对整个文本分词
        if (word not in stopwords) and len(word)>=2:
            if word not in word_count.keys(): word_count[word] = 1
            else: word_count[word] += 1
    #根据词频计算词语得分, 这里用了简单的归一化, 可以改进
    for key in word_count.keys():
        word_count[key] = word_count[key] / max(word_count.values())
    #摘取文本中所有句子
    text_sentences_0 = ''.join( re.findall(r'[\u4e00-\u9fa5|0-9|！|\!|？|\?|，|,|。|\.|、|@|#|￥|$|%|&|*|（|）|—|+|-|=|【|】|；|：|‘|’|“|”|《|》]',text_org) )
    text_sentences_0 = re.split(r'([。|！|\!|\.|？|\?]+)', text_sentences_0)
    text_sentences = []
    for i in range(len(text_sentences_0)//2):
        text_sentences.append(text_sentences_0[2*i]+text_sentences_0[2*i+1])
    #将词语得分加总得到句子得分
    sentence_score = {}
    for sentence in text_sentences: 
        text_words_in_sentence = jieba.cut(sentence)
        for word in text_words_in_sentence:
            if word in word_count.keys():
                if len(sentence)<100:   #太长的句子不合适
                    if sentence not in sentence_score.keys(): sentence_score[sentence] = word_count[word]
                    else: sentence_score[sentence] += word_count[word]

    sentence_score_df = pd.DataFrame([sentence_score.keys(),sentence_score.values()]).T
    sentence_score_df = sentence_score_df.sort_values([1],ascending=False)
    result = sentence_score_df.iloc[:sentence_num,0]

    return result

'''
text_org = '新华社北京5月15日电 题：首套房贷利率下限下调影响几何？新华社记者吴雨15日，中国人民银行、银保监会发布通知，调整首套住房商业性个人住房贷款利率下限。这是贷款市场报价利率改革以来，首次下调首套房贷款利率下限。当日人民银行、银保监会发布的通知称，对于贷款购买普通自住房的居民家庭，首套住房商业性个人住房贷款利率下限调整为不低于相应期限贷款市场报价利率（LPR）减20个基点，二套住房商业性个人住房贷款利率政策下限按现行规定执行。图片来自中国人民银行网站截图　　图片来自中国人民银行网站截图在此之前，全国层面执行的利率下限是首套房贷利率不得低于相应期限的LPR，二套房贷利率不得低于相应期限LPR加60个基点。政策调整后，按照4月20日发布的5年期以上LPR测算，首套房贷利率不得低于4.4%。记者了解到，此次政策调整主要针对新发放商业性个人住房贷款，存量商业性个人住房贷款利率仍按原合同执行。也就是说，如所在城市政策下限和银行具体执行利率跟随全国政策同步下调，居民家庭申请贷款购买首套普通自住房时，利息支出会有所减少。我国房地产市场区域特征明显，个人住房贷款利率和首付比例确定遵循因城施策原则，并采用全国、城市、银行三层的定价机制。不过，在实践中，多数城市直接采用了全国政策下限，没有额外再做加点要求。此次全国政策下限调整后，各地政策是否会同步调整？其实，3月份以来，由于市场需求减弱，房贷利率已经历过一波调整，且主要是发生在银行层面。中国人民银行金融市场司司长邹澜介绍，全国已经有一百多个城市的银行根据市场变化和自身经营情况，自主下调了房贷利率，平均幅度在20个到60个基点不等。同时，部分省级市场利率定价自律机制也配合地方政府的调控要求，根据城市实际情况，在全国政策范围内，下调了本城市首付比例下限和利率下限。“这是城市政府、银行根据市场形势和自身经营策略，做出的差别化、市场化调整，适应了房地产市场区域差异的特征。”邹澜说。相关数据显示，当前商品住宅销售额出现下滑，个人住房贷款发放额也有小幅回落。新发布的金融统计数据表明，4月住户贷款减少2170亿元。其中，住房贷款减少605亿元。此次通知明确提出，在全国统一的贷款利率下限基础上，人民银行、银保监会各派出机构按照“因城施策”的原则，指导各省级市场利率定价自律机制，根据辖区内各城市房地产市场形势变化及城市政府调控要求，自主确定辖区内各城市首套和二套住房商业性个人住房贷款利率加点下限。专家认为，在城市贷款利率下限基础上，下一步银行业金融机构将结合自身经营情况、客户风险状况和信贷条件等因素，合理确定每笔贷款的具体加点数值。人民银行表示，房贷政策调整旨在坚持房子是用来住的、不是用来炒的定位，全面落实房地产长效机制，支持各地从当地实际出发完善房地产政策，支持刚性和改善性住房需求，促进房地产市场平稳健康发展。'
result = generate_summary(text_org, sentence_num=1)
'''





