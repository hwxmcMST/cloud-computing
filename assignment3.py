
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
import datetime
import numpy as np
from matplotlib import pyplot as plt
from sklearn import linear_model

sc=SparkContext.getOrCreate()
sql_sc=SQLContext(sc)

df=pd.read_csv("nyc_taxi.csv",sep=',')
duration=[]
for i in range(0,df.shape[0]):
    month,day,year=df.iloc[i]['pickup_date'].split('/')
    hour,minute=df.iloc[i]['pickup_time'].split(':')
    pickup=datetime.datetime(int(year),int(month),int(day),int(hour),int(minute))
    
    month,day,year=df.iloc[i]['dropoff_date'].split('/')
    hour,minute=df.iloc[i]['dropoff_time'].split(':')
    dropoff=datetime.datetime(int(year),int(month),int(day),int(hour),int(minute))
    
    dur=dropoff-pickup
    duration.append(divmod(dur.seconds,60)[0])
df.insert(5,'duration',duration)
   
data_df = sql_sc.createDataFrame(df)

##1
###The first model with one feature  
parsed_data1=data_df.rdd.map(lambda x: LabeledPoint(x[7], x[4:5]))
lr_model1 = LinearRegressionWithSGD.train(parsed_data1,iterations=100,step=0.01)
prediction1 = lr_model1.predict([20])
print("The predicted value of 20 mile: "+ str(prediction1))
##The predicted value of 20 mile: 77.7861968125786


##2
##compute the average tip
tip_df=data_df.select(data_df.tip)
map_rdd=tip_df.rdd.map(lambda x:(int(x[0]),1))
sumCount = map_rdd.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print("The average tip is " + str(sumCount[0]/sumCount[1]))
##The average tip is 1.45843577443566 

##3
##Compute during which hour the city experiences the most number of trips 
time_df=data_df.select(data_df.pickup_time)##using the pickup_time column
time_map_rdd=time_df.rdd.map(lambda x:x[0].split(':')[0])
time_map_rdd=time_map_rdd.map(lambda t:(t,1))
time_reduce_rdd=time_map_rdd.reduceByKey(lambda x,y:x+y)
max_value=time_reduce_rdd.top(1)
print("The most number of trips is during "+max_value[0][0]+" and "+str(int(max_value[0][0])+1))
##The most number of trips is during 9 and 10 





    

