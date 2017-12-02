import json
import os
from pyspark import SparkContext
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

sc = SparkContext()
analyser = SentimentIntensityAnalyzer()
MAX_SCALE = 10.0  # max rating
inputPath = "./prediction.json"  # input path
outputPath = "./score.json"  # output path
jsonIndent = 2  # json format control


def display(x):
    print x


def readJSON():
    rawData = sc.textFile(inputPath)
    RDD = rawData.map(lambda x: (
        json.loads(x).get("asin"), json.loads(x).get("reviewType"), json.loads(x).get("reviewText"),
        json.loads(x).get("overall"), json.loads(x).get("helpful")
    ))
    print "transfer raw data to RDD - complete"
    return RDD


def getSentimentScore(review):
    return analyser.polarity_scores(review)['compound']


def getRating(rating):
    return float(rating)


def getHelpfulness(x):
    total = x.split(', ')[1].split(']')[0]
    if total == "0":
        return 1
    useful = x.split(', ')[0].split('[')[1]
    return float(useful) / float(total)


def getScore(data):
    sentimentScore = getSentimentScore(data[2])
    rating = getRating(data[3])
    helpfulness = getHelpfulness(data[4])
    base = (rating * 2 + 1) / 12.0
    scaledSentimentScore = sentimentScore * helpfulness
    score = base + scaledSentimentScore / 12.0
    return score * MAX_SCALE


def generateScore(RDD):
    RDD = RDD.map(lambda x: (x[0], x[1], getScore(x), 1)).keyBy(lambda x: x[0] + x[1]).reduceByKey(
        lambda x, y: (x[0], x[1], x[2] + y[2], x[3] + y[3])).map(
        lambda x: (x[1][0], x[1][1], x[1][2] / x[1][3])).sortBy(lambda x: x[1]).sortBy(lambda x: x[0])
    # RDD.foreach(display)
    print "generate score for each review and merge reviews for each item under same category - complete"
    return RDD


def write(data):
    dictionary = {}
    dictionary['asin'] = data[0]
    dictionary['reviewType'] = data[1]
    dictionary['score'] = data[2]
    with open(outputPath, "a") as writer:
        json.dump(dictionary, writer, indent=jsonIndent)


def writeJSON(RDD):
    if os.path.exists(outputPath):
        os.remove(outputPath)
    RDD.foreach(write)
    print "write result to json output - complete"


def main():
    print "start"
    RDD = readJSON()
    RDD.cache()
    RDD = generateScore(RDD)
    writeJSON(RDD)
    print "end"


if __name__ == "__main__":
    main()
