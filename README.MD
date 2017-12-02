
### SelectTrainingSet.java SelectTrainingSetSingleKeyword.java
To run SelectTrainingSet.java or SelectTrainingSetSingleKeyword.java:
Modify line 22, 23, 24 into the file path/name you want to open and write into.
Run and a new file will be generated.

### sample_trainset.py
Use command 'bin\spark-submit --master local PATH_TO_SCRIPT' to execute this script.
The purpose of this script is to sample a subset for training from the labeled datasets.
Path of input datasets, sampling probability and the number of instances to be sampled can be specified in the code.

### experiment.py
Use command 'bin\spark-submit --master local PATH_TO_SCRIPT' to execute this script.
This script is to run experiments with selected training sets and classifiers.
Comment/uncooment code blocks to select which experiment to be conducted.

### classify.py
Use command 'bin\spark-submit --master local PATH_TO_SCRIPT' to execute this script.
The purpose of this script is to classify unlabeled data.
File paths can be specified in the code.

### score.py   
#### How to calculate it   
The goal is to firstly map original user ratings to several new base scores, and change this base score by amending score (new score = mapped base score + amending score)  
The original user score is from 0.0 to 5.0, we want (1) mapped_score(0.0) + amending_score.MIN_VALUE = 0, (2) mapped_score(5.0) + amending_score.MAX_VALUE = 1 and (3) amending_score.MIN_VALUE = - amending_score.MAX_VALUE.  
So the mapping will be: map(x) = (2x+1)/12, with amending_score.MAX_VALUE = 1/12 and amending_score.MIN_VALUE = -1/12.  
For amending score, it will be: sentiment score * helpfulness * 1/12. sentiment score calculated by vader will be in (-1, 1); helpfulness will be the fraction of people who think the review is helpful, or 1 if no one rated this in case of run time exception.