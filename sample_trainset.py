import ast
import nltk
import string
import numpy as np
import json
from pyspark import SparkContext

'''
	Author: Yaxiong Liu
	Reference: Analyzing Amazon Reviews - Akanksha, Arunachalam, A., Sethuraman, M., Srinivasaragavan, N.
	Note: To execute the python script, please the the command in format "bin\spark-submit --master local PATH_TO_SCRIPT"
'''

# The paths for the input files
# INPUT_FILES=['./project/datasets/MK/Baby_5_11-1_TS.json', './project/datasets/MK/Beauty_5_11-1_TS.json',\
# 	'./project/datasets/MK/CDs_and_Vinyl_5_11-1_TS.json','./project/datasets/MK/Cell_Phones_and_Accessories_5_11-1_TS.json',\
# 	'./project/datasets/MK/Clothing_Shoes_and_Jewelry_5_11-1_TS.json','./project/datasets/MK/Electronics_5_11-1_TS.json',\
# 	'./project/datasets/MK/Grocery_and_Gourmet_Food_5_11-1_TS.json','./project/datasets/MK/Health_and_Personal_Care_5_11-1_TS.json',\
# 	'./project/datasets/MK/Home_and_Kitchen_5_11-1_TS.json','./project/datasets/MK/Musical_Instruments_5_11-1_TS.json',\
# 	'./project/datasets/MK/Office_Products_5_11-1_TS.json','./project/datasets/MK/Patio_Lawn_and_Garden_5_11-1_TS.json',\
# 	'./project/datasets/MK/Pet_Supplies_5_11-1_TS.json','./project/datasets/MK/Sports_and_Outdoors_5_11-1_TS.json',\
# 	'./project/datasets/MK/Tools_and_Home_Improvement_5_11-1_TS.json','./project/datasets/MK/Toys_and_Games_5_11-1_TS.json']
INPUT_FILES=['./project/datasets/SK/Baby_5-SK-TS.json', './project/datasets/SK/Beauty_5-SK-TS.json',\
	'./project/datasets/SK/CDs_and_Vinyl_5-SK-TS.json','./project/datasets/SK/Cell_Phones_and_Accessories_5-SK-TS.json',\
	'./project/datasets/SK/Clothing_Shoes_and_Jewelry_5-SK-TS.json','./project/datasets/SK/Electronics_5-SK-TS.json',\
	'./project/datasets/SK/Grocery_and_Gourmet_Food_5-SK-TS.json','./project/datasets/SK/Health_and_Personal_Care_5-SK-TS.json',\
	'./project/datasets/SK/Home_and_Kitchen_5-SK-TS.json','./project/datasets/SK/Musical_Instruments_5-SK-TS.json',\
	'./project/datasets/SK/Office_Products_5-SK-TS.json','./project/datasets/SK/Patio_Lawn_and_Garden_5-SK-TS.json',\
	'./project/datasets/SK/Pet_Supplies_5-SK-TS.json','./project/datasets/SK/Sports_and_Outdoors_5-SK-TS.json',\
	'./project/datasets/SK/Tools_and_Home_Improvement_5-SK-TS.json','./project/datasets/SK/Toys_and_Games_5-SK-TS.json']
# INPUT_FILES=['./project/datasets/SK/Electronics_5-SK-TS.json']
# INPUT_FILES=['./project/datasets/SK/CDs_and_Vinyl_5-SK-TS.json','./project/datasets/SK/Electronics_5-SK-TS.json']
# INPUT_FILES=['./project/datasets/SK/Baby_5-SK-TS.json', './project/datasets/SK/Beauty_5-SK-TS.json',\
# 	'./project/datasets/SK/CDs_and_Vinyl_5-SK-TS.json','./project/datasets/SK/Cell_Phones_and_Accessories_5-SK-TS.json',\
# 	'./project/datasets/SK/Clothing_Shoes_and_Jewelry_5-SK-TS.json','./project/datasets/SK/Electronics_5-SK-TS.json',\
# 	'./project/datasets/SK/Grocery_and_Gourmet_Food_5-SK-TS.json','./project/datasets/SK/Health_and_Personal_Care_5-SK-TS.json']

# The path for the output file
OUTPUT_FILE='./project/trainset.json'

# The probility to sample each instance
QUALITY_PROB,APPEARANCE_PROB,DURABILITY_PROB,SHIPMENT_PROB,IMAGE_PROB=0.04,0.03,0.24,0.12,0.12

# The maximum number of instances to be sampled for each type.
MAX_COUNT=16000

f = open(OUTPUT_FILE,'w')
sc=SparkContext()

def process_text(text):
	''' get review text and review type from training set json, tokenize the reviews and remove stop words and punctuations

		Args:
			text (str): review text to be processed.

		Return:
			a tuple
			 - 1st entry: ASIN number of the product
			 - 2nd entry: the rating from the review
			 - 3rd entry: the original review text 
			 - 4th entry: the type for the review
			 - 5th entry: the helpfulness score of the ewview

	'''
	curr_json = ast.literal_eval(text.strip())
	review_text = curr_json["reviewText"]
	category = curr_json['reviewType']
	rating = curr_json["overall"]
	asin = curr_json['asin']
	helpful = curr_json['helpful']
	return (asin,rating,review_text,category,helpful)

''' 
	Process each file in the input files and count the number for each review type
	Each review is sampled with probability depending on its review type
'''
quality_total_count,appr_total_count,durab_total_count,ship_total_count,image_total_count,other_total_count=0,0,0,0,0,0
quality_count,appr_count,durab_count,ship_count,image_count,other_count=0,0,0,0,0,0
for FILE in INPUT_FILES:
	print "Processing...",FILE
	textFile = sc.textFile(FILE)
	data = textFile.flatMap(lambda line: line.split("\n"))
	rdd = data.map(lambda line: process_text(line))
	for curr_data in rdd.collect():
		curr_dict = {}
		curr_dict['asin'] = str(curr_data[0])
		curr_dict['overall'] = str(curr_data[1])
		curr_dict['reviewText'] = str(curr_data[2])
		curr_dict['helpful'] = str(curr_data[4])
		if str(curr_data[3])=='quality': 
			quality_total_count+=1
			if quality_count<MAX_COUNT and np.random.randint(100)<100*QUALITY_PROB:
				quality_count+=1
				curr_dict['reviewType'] = 'quality'
				json_data = json.dumps(curr_dict)
				f.write(str(json_data)+'\n')
		elif str(curr_data[3])=='appearance': 
			appr_total_count+=1
			if appr_count<MAX_COUNT and np.random.randint(100)<100*APPEARANCE_PROB:
				appr_count+=1
				curr_dict['reviewType'] = 'appearance'
				json_data = json.dumps(curr_dict)
				f.write(str(json_data)+'\n')
		elif str(curr_data[3])=='durability': 
			durab_total_count+=1
			if durab_count<MAX_COUNT and np.random.randint(100)<100*DURABILITY_PROB:
				durab_count+=1
				curr_dict['reviewType'] = 'durability'
				json_data = json.dumps(curr_dict)
				f.write(str(json_data)+'\n')
		elif str(curr_data[3])=='shipment': 
			ship_total_count+=1
			if ship_count<MAX_COUNT and np.random.randint(100)<100*SHIPMENT_PROB:
				ship_count+=1
				curr_dict['reviewType'] = 'shipment'
				json_data = json.dumps(curr_dict)
				f.write(str(json_data)+'\n')
		elif str(curr_data[3])=='image reliability': 
			image_total_count+=1
			if image_count<MAX_COUNT and np.random.randint(100)<100*IMAGE_PROB:
				image_count+=1
				curr_dict['reviewType'] = 'image reliability'
				json_data = json.dumps(curr_dict)
				f.write(str(json_data)+'\n')
		else: 
			other_total_count+=1
	print "Complete...",FILE

# print the count
print "Result:"
print 'quality',quality_count,quality_total_count
print 'appearance',appr_count,appr_total_count
print 'durability',durab_count,durab_total_count
print 'shipment',ship_count,ship_total_count
print 'image reliability',image_count,image_total_count
print 'other',other_count,other_total_count

f.close()