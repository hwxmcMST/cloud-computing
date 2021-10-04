#!/usr/bin/env python
# coding: utf-8

# In[6]:


import json


# In[8]:


all_file="file://" +'/data2/sunandan/sparkex/h516/assignment2/data/*'


# In[10]:


inputR=sc.textFile(all_file)
inputR.take(1)
dataR = inputR.map(lambda x: json.loads(x))
dataR=dataR.map(lambda x:x['text'])
#dataR.take(3)


# In[11]:


data_map=dataR.flatMap(lambda x:x.lower().split())
data_map=data_map.map(lambda x:(x,1))
data_reduce=data_map.reduceByKey(lambda x,y:x+y)
#data_reduce.count()
data_filter=data_reduce.filter(lambda x:x[1]>=10)
data_filter.count()


# In[42]:


##Question 2
search_list=['congress', 'london', 'washington', 'football']
data_filter=data_filter.map(lambda x:(x[1],x[0]))
data_filter=data_filter.sortByKey()
for i in search_list:
    print(i+": "+str(data_filter.lookup(i)))


# In[7]:


##Question 3
month_file=['01-*','02-*','03-*','04-*','05-*','06-*','07-*','08-*','09-*','10-*','11-*','12-*']
monthRDD=[]
monthR=[]
for i in range(0,len(month_file)):
    month_file[i]="file://" +'/data2/sunandan/sparkex/h516/assignment2/data/2012-'+month_file[i]
    monthRDD.append(sc.textFile(month_file[i]).map(lambda x:json.loads(x)))
    monthRDD[i]=monthRDD[i].map(lambda x:x['text'])
    monthRDD[i]=monthRDD[i].flatMap(lambda x:x.lower().split())
    monthR.append(monthRDD[i].map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y))
    monthR[i]=monthR[i].filter(lambda x:x[1]>=10)
    monthR[i]=monthR[i].map(lambda x:(x[1],x[0]))
    print("Month "+ str(i+1)+": "+str(monthR[i].top(1)))
                


# In[8]:


##Question 4
file1="file://" +'/data2/sunandan/sparkex/h516/assignment2/data/2012-08-01'
file2="file://" +'/data2/sunandan/sparkex/h516/assignment2/data/2012-09-01'
RDD1=sc.textFile(file1).map(lambda x:json.loads(x)).map(lambda x:x['text'])
RDD2=sc.textFile(file2).map(lambda x:json.loads(x)).map(lambda x:x['text'])
RDD1=RDD1.flatMap(lambda x:x.lower().split())
RDD2=RDD2.flatMap(lambda x:x.lower().split())
inter_RDD=RDD1.intersection(RDD2)
sub_RDD=RDD2.subtract(inter_RDD)
sub_RDD.collect()


# In[9]:


##Question5
print('The frequency of word "monsoon" for all months: ')
for i in range(0,len(month_file)):
    #monthR[i]=monthR[i].map(lambda x:(x[1],x[0]))
    monthR[i]=monthR[i].sortByKey(keyfunc=lambda x: x[0])
    #print(monthR[i].top(1))
    print("Month "+str(i+1)+": "+monthR[i].lookup('monsoon'))


# In[ ]:





# In[ ]:




