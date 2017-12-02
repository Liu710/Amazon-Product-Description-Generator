import ast
import nltk
import string
import json
import numpy as np
import datetime

from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from nltk.corpus import stopwords
from sklearn.svm import SVC,LinearSVC

'''
	Author: Yaxiong Liu
	Reference: Analyzing Amazon Reviews - Akanksha, Arunachalam, A., Sethuraman, M., Srinivasaragavan, N.
	Note: To execute the python script, please the the command in format "bin\spark-submit --master local PATH_TO_SCRIPT"
'''
print "Start " + str(datetime.datetime.now().time())

# Path to the training set
TRAIN_FILE="./project/trainset.json"
# Path to the testing set
TEST_FILE="./project/datasets/Automotive_5.json"
# Path to the output file
OUTPUT_FILE="./project/prediction.json"
# Select the model
model=LinearSVC()

#==========================================================================================================================
# Step 1: Read the training and testing data, and other neccessary data
#==========================================================================================================================
sc=SparkContext()
train_textFile = sc.textFile(TRAIN_FILE)
test_textFile = sc.textFile(TEST_FILE)

#==========================================================================================================================
# Step 2: Process train and test file
#==========================================================================================================================
train_data = train_textFile.flatMap(lambda line: line.split("\n"))
test_data = test_textFile.flatMap(lambda line: line.split("\n"))
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

def process_test_text(text): 
	''' get neccessary fields from testing set json, tokenize the reviews and remove stop words and punctuations

		Args:
			text (str): review text to be processed.

		Return:
			a tuple
			 - 1st entry: ASIN number of the product
			 - 2nd entry: the original review text
			 - 3rd entry: the rating from the review
			 - 4th entry: the helpfulness score of the ewview
			 - 5th entry: the processed review text
	'''
	curr_json = ast.literal_eval(text.strip())
	old_text = curr_json["reviewText"]
	review_text = nltk.word_tokenize(curr_json["reviewText"])
	review_text = [token for token in review_text if token not in engStopWords and token not in string.punctuation]
	review_text = ' '.join(review_text)
	rating = curr_json["overall"]
	asin = curr_json['asin']
	helpful = curr_json['helpful']
	return (asin,old_text,rating,helpful,review_text)

# Get RDD for training set and testing set
train_rdd = train_data.map(lambda line: process_train_text(line))
test_rdd = test_data.map(lambda line: process_test_text(line))

#===================================================================================================================
# Step 3: Build the Text features
#===================================================================================================================
getTFVector = lambda s: HashingTF(1000).transform(s.split()).toArray()
train_tf_rdd = train_rdd.map(lambda x: (getTFVector(x[0]), x[1]))
test_tf_rdd = test_rdd.map(lambda x: (x[0],x[1],x[2],x[3],getTFVector(x[4]).reshape(1,-1)))

#===================================================================================================================
# Step 4: Train the selected model
#===================================================================================================================
train_features,train_labels = zip(*train_tf_rdd.collect())
print "Start Training " + str(datetime.datetime.now().time())
trained_model = model.fit(train_features, train_labels)
print "End Training " + str(datetime.datetime.now().time())
#===================================================================================================================
# Step 4.5: cross-validation
#===================================================================================================================
# print cross_val_score(model, train_features, train_labels, cv=5)

# #===================================================================================================================
# # Step 5: Get the prediction RDD for the reviews in the test data
# #===================================================================================================================
pred_rdd = test_tf_rdd.map(lambda x: (x[0],x[1],x[2],x[3],trained_model.predict(x[4])[0]))

# #===================================================================================================================
# # Step 6: Output the prediction result
# #===================================================================================================================
f = open(OUTPUT_FILE,'w')
for curr_data in pred_rdd.collect():
	curr_dict = {}
	curr_dict['asin'] = str(curr_data[0])
	curr_dict['reviewText'] = str(curr_data[1])
	curr_dict['overall'] = str(curr_data[2])
	curr_dict['helpful'] = str(curr_data[3])
	curr_dict['reviewType'] = str(curr_data[4])
	json_data = json.dumps(curr_dict)
	f.write(str(json_data)+'\n')
f.close()

print "End " + str(datetime.datetime.now().time())