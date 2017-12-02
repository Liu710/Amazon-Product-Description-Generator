import ast
import nltk
import string
import json
import numpy as np
import datetime
from time import time

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from nltk.corpus import stopwords
from sklearn.naive_bayes import GaussianNB,MultinomialNB,BernoulliNB
from sklearn.svm import SVC,LinearSVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score

'''
	Author: Yaxiong Liu
	Reference: Analyzing Amazon Reviews - Akanksha, Arunachalam, A., Sethuraman, M., Srinivasaragavan, N.
	Note: To execute the python script, please the the command in format "bin\spark-submit --master local PATH_TO_SCRIPT"
'''
# Path
TRAIN_FILE="./project/trainset.json"

print "Start Experiments " + str(datetime.datetime.now().time())
print "=================================================================="

#==========================================================================================================================
# Step 1: Read the training and testing data, and other neccessary data
#==========================================================================================================================
sc=SparkContext()
train_textFile = sc.textFile(TRAIN_FILE)

#==========================================================================================================================
# Step 2: Process training set
#==========================================================================================================================
train_data = train_textFile.flatMap(lambda line: line.split("\n"))

engStopWords=stopwords.words("english") + ['\'s'] # Get all of the stop words

def process_train_text(text): 
	''' get review text and review type from training set json, tokenize the reviews and remove stop words and punctuations

		Args:
			text (str): review text to be processed.

		Return:
			a tuple
			 - 1st entry: the processed review text
			 - 2nd entry: the type of the review

	'''

	curr_json = ast.literal_eval(text.strip())
	review_text = nltk.word_tokenize(curr_json["reviewText"])
	review_text = [token for token in review_text if token not in engStopWords and token not in string.punctuation]
	review_text = ' '.join(review_text)
	reviewType = curr_json['reviewType']
	return (review_text,reviewType)

# Get RDD for training set and testing set
train_rdd = train_data.map(lambda line: process_train_text(line))

#===================================================================================================================
# Step 3: Build the Text features
#===================================================================================================================
getTFVector = lambda s: HashingTF(1000).transform(s.split()).toArray()
train_tf_rdd = train_rdd.map(lambda x: (getTFVector(x[0]), x[1]))


#===================================================================================================================
# Step 4: Train the selected model and cross-validation (uncomment for enable the experiment for each classifier)
#===================================================================================================================
train_features,train_labels = zip(*train_tf_rdd.collect())

# print "===================== Decision Tree =============================="
# startTime=time()
# model=DecisionTreeClassifier()
# print "Start Training "
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(time()-startTime)
# print cross_val_score(model, train_features, train_labels, cv=10)

# print "===================== Random Forest =============================="
# startTime=time()
# model=RandomForestClassifier()
# print "Start Training "
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(time()-startTime)
# print cross_val_score(model, train_features, train_labels, cv=10)

# print "===================== SVC Linear ================================="
# startTime=time()
# model=LinearSVC()
# print "Start Training "
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(time()-startTime)
# print cross_val_score(model, train_features, train_labels, cv=10)

# print "===================== SVM Linear ================================="
# model=SVC(kernel='linear', max_iter=1000, verbose=True)
# print "Start Training " + str(datetime.datetime.now().time())
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(datetime.datetime.now().time())
# print cross_val_score(model, train_features, train_labels, cv=5)

# print "===================== SVM Poly ==================================="
# model=SVC(kernel='poly', verbose=True)
# print "Start Training " + str(datetime.datetime.now().time())
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(datetime.datetime.now().time())
# print cross_val_score(model, train_features, train_labels, cv=5)

# print "===================== SVM RBF ===================================="
# model=SVC(kernel='rbf', verbose=True)
# # print "Start Training " + str(datetime.datetime.now().time())
# # trained_model = model.fit(train_features, train_labels)
# # print "End Training " + str(datetime.datetime.now().time())
# print cross_val_score(model, train_features, train_labels, cv=5)

# print "===================== SVM Sigmoid ================================"
# model=SVC(kernel='sigmoid')
# # print "Start Training " + str(datetime.datetime.now().time())
# # trained_model = model.fit(train_features, train_labels)
# # print "End Training " + str(datetime.datetime.now().time())
# print cross_val_score(model, train_features, train_labels, cv=5)

# print "===================== MultinomialNB =============================="
# startTime=time()
# model=MultinomialNB()
# print "Start Training "
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(time()-startTime)
# print cross_val_score(model, train_features, train_labels, cv=10)

# print "===================== GaussianNB ================================="
# startTime=time()
# model=GaussianNB()
# print "Start Training "
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(time()-startTime)
# print cross_val_score(model, train_features, train_labels, cv=10)

# print "===================== BernoulliNB ================================"
# startTime=time()
# model=BernoulliNB()
# print "Start Training "
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(time()-startTime)
# print cross_val_score(model, train_features, train_labels, cv=10)

# print "===================== KNN ==========================================="
# model=KNeighborsClassifier()
# print "Start Training " + str(datetime.datetime.now().time())
# trained_model = model.fit(train_features, train_labels)
# print "End Training " + str(datetime.datetime.now().time())
# print cross_val_score(model, train_features, train_labels, cv=5)

print "=================================================================="
print "End Experiments" + str(datetime.datetime.now().time())