#Look at this page for information
#http://scikit-learn.org/stable/auto_examples/linear_model/plot_ols.html#sphx-glr-auto-examples-linear-model-plot-ols-py

from sklearn import linear_model
import datetime as dt

#start time
start = dt.datetime.now()


#Reading the data
data =  [k.strip().split(',') for k in open('data/nyc_taxi.csv','r').readlines()[1:]]

#Converting the features into float and putting them in a list
# Model M1
feature = []
label = []
for d in data:
	feature.append([float(d[-3])])
	label.append(float(d[-1]))

#Training
reg = linear_model.LinearRegression()
reg.fit (feature, label)

#predict 2 mile trip fare
print reg.predict([2])

#end time
end = dt.datetime.now()

print end-start

# repeat this for M2
