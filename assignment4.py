#!/usr/bin/env python3
# -*- coding: utf-8 -*-


##export JAVA_HOME=~/jre1.8.0_231
##export PYSPARK_PYTHON=/usr/bin/python3
##export PYSPARK_DRIVER_PYTHON=ipython3

from pyspark import SparkContext, SparkConf, SQLContext

conf=SparkConf().setAppName('SentimentAnalysis')
sc=SparkContext.getOrCreate(conf=conf)
sql_sc=SQLContext(sc)

pos_file=open('pos.txt','r')
pos=pos_file.read().split('\n')

neg_file=open('neg.txt','r')
neg=neg_file.read().split('\n')

all_file='moviereviews'

inputR=sc.wholeTextFiles(all_file)

data_map=inputR.flatMap(lambda x: [(x[0],i) for i in x[1].lower().split()])

data_pos=data_map.filter(lambda x: x[1] in pos)
data_neg=data_map.filter(lambda x: x[1] in neg)

data_pos_map=data_pos.map(lambda p:((p[0],p[1]),1))
data_neg_map=data_neg.map(lambda n:((n[0],n[1]),1))

data_pos_reduce=data_pos_map.reduceByKey(lambda x,y: x+y)
data_neg_reduce=data_neg_map.reduceByKey(lambda x,y: x+y)

pos_score=data_pos_reduce.map(lambda x:((x[0][0],'pos'),x[1])).reduceByKey(lambda x,y:x+y)
neg_score=data_neg_reduce.map(lambda x:((x[0][0],'neg'),x[1])).reduceByKey(lambda x,y:x+y)

pos_score=pos_score.map(lambda x: (x[0][0], (x[0][1],x[1])))
neg_score=neg_score.map(lambda x: (x[0][0], (x[0][1],x[1])))

final_score=pos_score.join(neg_score)
final_score=final_score.map(lambda f:(f[0],f[1][0][1]-f[1][1][1]))
print(final_score.take(3))
final_sentiment=final_score.map(lambda f:(f[0],'positive') if f[1]>0 \
                                else (f[0],'negative'))
print(final_sentiment.take(3))


