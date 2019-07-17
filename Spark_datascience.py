###########
########################################
#Starting code for setting up the environment.
import pyspark
import os
import sys
os.chdir('/Users/ruanhq/Desktop/Spark/data')
os.environ['SPARK_HOME'] = '/Users/ruanhq/Desktop/Spark/spark-2.4.3-bin-hadoop2.7'
SPARK_HOME =os.environ['SPARK_HOME']
sys.path.insert(0, os.path.join(SPARK_HOME, 'python'))
sys.path.insert(0, os.path.join(SPARK_HOME, 'python','lib'))
sys.path.insert(0, os.path.join(SPARK_HOME, 'python', 'lib', 'pyspark.zip'))
sys.path.insert(0, os.path.join(SPARK_HOME, 'python', 'lib', 'py4j-0.10.7-src.zip'))


from pyspark.sql import SparkSession
from pyspark import SparkContext

#Starting a spark session:
SpSession = SparkSession \
.builder \
.master('local[2]')\
.appName('ruanhqspark')\
.config('spark.executor.memory', '1g')\
.config('spark.cores.max', '2')\
.config('spark.sql.warehouse.dir', '/Users/ruanhq/Spark/spark-warehouse').getOrCreate()

SpContext = SpSession.sparkContext

########################################




#######

##Operation: load and store data:
autodata = SpContext.textFile('auto-data.csv')
autodata.cache()
first_line = autodata.first()
datalines = autodata.filter(lambda X: X !=first_line)
datalines.count()

for line in autodata.collect():
	print(line)
#Very computational costly, only for small datasets.

#Save to a local file: first collect the RDD to the master and then save as a local file.
autodatafile = open('auto-data-saved.csv', 'w')
autodatafile.write('\n'.join(autodata.collect()))
autodatafile.close()

#Load the iris data:
iris_data = SpContext.textFile('iris.csv')
iris_data.cache()
for sample in iris_data.collect():
	print(sample)
#############
#Perform operation on one RDD and create a new RDD:
#Operate on one element at a time, perform operation on one RDD and create a new RDD.
newRdd = rdd.map(function)
#works similar to the map reduce 'map'.
#Data standardization, type conversion, compute tax, add new attributes.
newRdd = rdd.flatmap(function): 
#return more elements than the original map
#split strings in the original map, extract child elements from a nested json string.
#filter:
newRdd = rdd.filter(function)
#filter a RDD to select elements that match a condition.
#result RDD smaller than the original RDD
#A function can be passed as a condition to perform complex filtering.
#Set operation performed on two RDDs,
unionRDD = firstRDD.union(secondRDD)
intersectionRDD = firstRDD.intersect(secondRDD)
# return a new RDD that contains the intersection of elements in the source dataset and the argument.

##################
tsvData = autodata.map(lambda X: X.replace(",", "\t"))
tsvData.take(5)

toyotaData = autodata.filter(lambda X: 'toyota' in X)
toyotaData.count()

words = toyotaData.flatMap(lambda line: line.split(","))
words.count()
words.take(20)

for numbData in collData.distinct().collect():
	print(numbData)


#Set operations:
words1 = SpContext.parallelize(['hello', 'war', 'peace', 'world'])
words2 = SpContext.parallelize(['war', 'peace', 'universe'])

for unions in words1.union(words2).distinct().collect():
	print(unions)

for intersects in words1.intersection(words2).collect():
	print(intersects)

#####
#Using functions for transformation:
def cleanseRDD(autoStr):
	if isinstance(autoStr, int):
		return autoStr
	attlist = autoStr.split(",")
	if attlist[3] == 'two':
		attlist[3] = '2'
	else:
		attlist[3] = '4'
	#Convert drive to uppercase:
	attlist[5] = attlist[5].upper()
	return ",".join(attlist)

cleanedData = autodata.map(cleanseRDD)
cleanedData.collect()


##Action:
#reduce, func(func(a, b))
collData.collect()
collData.reduce(lambda x,y: x + y)
collData.collect()

#perform a population-wise operation:
autodata.reduce(lambda x, y: x if len(x) < len(y) else y)

def getMPG(autoStr):
	if isinstance(autoStr, int):
		return autoStr
	attlist = autoStr.split(',')
	if attlist[9].isdigit():
		return int(attlist[9])
	else:
		return 0

autodata.reduce(lambda x,y: getMPG(x) + getMPG(y)) / (autodata.count() - 1)
########
#Key-value RDD:
#Pair RDDs: special type of RDDs that can store key value pairs:
#mapvalues, flatmapvalues: generate multiple values with the same key.
#Create a key-value RDD of auto brand and horsepower:
cylData = autodata.map(lambda x: (x.split(",")[0], x.split(",")[7]))
cylData.take(5)
cylData.keys().collect()

#remove header row:
header = cylData.first()
#Filter out the first row:
cylHPData = cylData.filter(lambda line: line != header)
cylHPData.take(5)
cylHPData.keys().collect()

#Find average HP by brand:
#add a count 1 to each record:
addOne = cylHPData.mapValues(lambda x: (x, 1))
addOne.collect()

########
#summarize the values:  summarize them by each key:
brandValues = addOne.reduceByKey(lambda x, y: (int(x[0]) + int(y[0]),
	x[1] + y[1]))
brandValues.collect()
#find average by dividing HP total by counts total.
brandValues.mapValues(lambda x: round(int(x[0]) / int(x[1]))).collect()
#Here each key only appear once.
#first add value, second add count:
#Here is equivalent to SUM(A) GROUP BY AA
#k2 = A.filter(lambda X: X != header)
########
#local variables in Spark:
#spark make copies of the code and execute 
#a read-only variable that is shared by all nodes, used for lookup tables or similar functions.
#spark optimizes distribution and storage for better performance.
####
#1. accumulators:
#2. partitioning:
#3. Can be specified during RDD creation explicitly.
#4. Persistence: 
#5.
######
#initialize accumulator
sedanCount = SpContext.accumulator(0)
hatchbackCount = SpContext.accumulator(0)

#Set broadcast variable:
sedanText = SpContext.broadcast('sedan')
hatchbackText = SpContext.broadcast('hatchback')

#splitting the lines:
def splitLines(line):
	global sedanCount
	global hatchbackCount
	#####
	if sedanText.value in line:
		sedanCount += 1
	if hatchbackText.value in line:
		hatchbackCount += 1
	return line.split(',')

#do the map:
splitData = autodata.map(splitLines)
splitData.collect()

splitData.count()

print(sedanCount, hatchbackCount)

#####
#Get partitions:
collData.getNumPartitions()

#
collData = SpContext.parallelize([3, 5, 4, 7, 4], 4) #BY default 2.
collData.cache()
collData.count()
collData.getNumPartitions()
#Select the 




###########################


#Spark with SQL:
#read the json:
#Create a dataframe from a json file:
empDf = SpSession.read.json('customerData.json')
empDf.printSchema()
empDf.show()

#######
#Do dataframe queries:
empDf.select('name').show()
empDf.filter(empDf['age'] == 40).show()
empDf.groupBy('gender').count().show()
empDf.groupBy('deptid').agg({'salary':'avg', 'age': 'max'}).show()

#######
#create a dataframe from a list:
deptlist = [{'name': 'Sales', 'id': '100'}, {'name': 'Engineering', 'id': '200'}]
deptDf = SpSession.createDataFrame(deptlist)
deptDf.show()

#join the dataframe:
empDf.join(deptDf, empDf.deptid == deptDf.id).show()

#Cascading operations:
empDf.filter(empDf['age'] > 30).join(deptDf, empDf.deptid == deptDf.id).groupBy('deptid').agg({'salary' : 'avg','age':'max'}).show()
empDf.filter(empDf['age'] > 30).join(deptDf, empDf.deptid == deptDf.id).groupBy('deptid').agg({'salary' : 'max', 'age' : 'avg'}).show()


######
#Creating dataframe from RDD:

from pyspark.sql import Row
lines = SpContext.textFile('auto-data.csv')

#remove the first line:

datalines = lines.filter(lambda X: 'FUELTYPE' not in X)
datalines.count()
datalines.collect()
parts = datalines.map(lambda l: l.split(","))

autoMap = parts.map(lambda X: Row(make = X[0], body = p[4], hp = int(p[7])))

autoDf = SpSession.CreateDataFrame(autoMap)



#######
#Create dataframe directly from CSV:
#There should be header or the columns will be c_1, c_2, ..., c_11
autoDf1 = SpSession.read.csv('auto-data.csv', header = True)
autoDf1.filter(autoDf1['RPM'] < 5100).groupBy('DOORS').agg({'MPG-CITY': 'avg', 'MPG-HWY': 'avg'}).show()

autoDf1.filter()




#####
#Creating and work with temp tables:
autoDf.createOrReplaceTempView('autos')
SpSession.sql('select * from autos where hp > 200').show()

empDf.createOrReplaceTempView('employees')
SpSession.sql('select * from employees where salary > 40000').show()


######
#Transform to pandas dataframe:
empPands = empDf.toPandas()
empPands.show()

for index, row in empPands.iterrows():
	print(row['salary'])


#######
#Working with databases:
demoDf = SpSession.read.format('jdbc').options(url = 'jdbc:mysql://localhost:3306/demo',
	driver = 'com.mysql.jdbc.Driver',
	dbtable = 'demotable',
	user = 'root',
	password = '').load()

demoDf.show()




#######################
#####Spark streaming:





from pyspark.streaming import StreamingContext

#Streaming with TCP/IP data:
#craete streaming context with latency of 1.
streamContext = StreamingContext(SpContext, 3)

totalLines = 0
lines = streamContext.socketTextStream('localhost', 1000)

#word counts in the data:
words = lines.flatMap(lambda line: line.split(' '))

pairs = words.map(lambda x: (x, 1))
wordcounts = pairs.reduceByKey(lambda x, y: x+ y)
wordcounts.pprint(5)


#Count lines:
totallines = 0
linescount = 0
def compute_metric(rdd):
	global totallines
	global linescount
	linescount = rdd.count()
	totallines += linescount
	print(rdd.collect())
	print('lines in RDD:', linescount, 'Total Lines', 'linescount')

lines.foreachRDD(compute_metric)

#compute window metrics:
def windowmetrics(rdd):
	print('window rdd size:', rdd.count())

windowedRDD = lines.window(6, 3)
windowedRDD.foreachRDD(windowMetrics)

streamContext.start()
streamContext.stop()





#######################
#Machine learning in Spark:
#Make ml scalable and easy.
#spark.mllib, spark.ml.

#Linear regression:

autoData = SpContext.textFile("auto-miles-per-gallon.csv")
autoData.cache()
autoData.take(5)
#Remove the first line (contains headers)
dataLines = autoData.filter(lambda x: "CYLINDERS" not in x)
dataLines.count()

"""--------------------------------------------------------------------------
Cleanup Data
-------------------------------------------------------------------------"""

from pyspark.sql import Row

#Use default for average HP
avgHP =SpContext.broadcast(80.0)

#Function to cleanup Data
def CleanupData( inputStr) :
    global avgHP
    attList=inputStr.split(",")
    
    #Replace ? values with a normal value
    hpValue = attList[3]
    if hpValue == "?":
        hpValue= float(80.0)
       
    #Create a row with cleaned up and converted data
    values= Row(     MPG=float(attList[0]),\
                     CYLINDERS=float(attList[1]), \
                     DISPLACEMENT=float(attList[2]), 
                     HORSEPOWER=float(hpValue),\
                     WEIGHT=float(attList[4]), \
                     ACCELERATION=float(attList[5]), \
                     MODELYEAR=float(attList[6]),\
                     NAME=attList[7]  ) 
    return values

#Run map for cleanup
autoMap = dataLines.map(CleanupData)
autoMap.cache()
autoMap.take(5)

#Create a Data Frame with the data. 
autoDf = SpSession.createDataFrame(autoMap)


#######
#Descriptive analysis:
autoDf.select('MPG', 'CYLINDERS').describe().show()

#Find correlation matrix between predictor and target:
#To extract the value: autoDf.select('COLUMN').take(1)[0][0]
for i in autoDf.columns:
	if not (isinstance(autoDf.select(i).take(1)[0][0], str)):
		print('Correlation to MPG for', i, autoDf.stat.corr('MPG', i))



#Prepare data for ML:

from pyspark.ml.linalg import Vectors
#transform to a dataframe for input to machine learning:
#drop columns that are not required(low correlation):

def transformToLabeledPoint(row):
	lp = (row['MPG'], Vectors.dense(row['ACCELERATION'],
		row['DISPLACEMENT'],
		row['WEIGHT']))
	return lp

autolp = autoMap.map(transformToLabeledPoint)
autoDf = SpSession.createDataFrame(autolp, ["label", "features"])
autoDf.select('label', 'features').show(5)

#Split the data into training and testing:

####
trainingData, testData = autoDf.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()

####
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(maxIter = 10)
lr_model = lr.fit(trainingData)

#####
print('Coefficient ' + str(lr_model.coefficients))
print('Intercept ' + str(lr_model.intercept))

#predict on test data:
predictions = lr_model.transform(testData)
predictions.select('prediction', 'label', 'features').show()

#Find R^2 for regression:
from pyspark.ml.evaluation import RegressionEvaluator

#Specify the label column, prediction column and the metric name:
evaluator = RegressionEvaluator(predictionCol = 'prediction',
	labelCol = 'label', metricName = 'r2')
evaluator.evaluate(predictions)

predic_in = lr_model.transform(trainingData)
evaluator.evaluate(predic_in)



#################
#DecisionTree classifier:

########
irisData = SpContext.textFile('iris.csv')
irisData.cache()
irisData.count()

datalines = irisData.filter(lambda X: 'Sepal' not in X)
datalines.count()

#####
from pyspark.sql import Row

parts = datalines.map(lambda l: l.split(','))
irisMap = parts.map(lambda X: Row(SEPAL_LENGTH = float(X[0]),
	SEPAL_WIDTH = float(X[1]),
	PETAL_LENGTH = float(X[2]),
	PETAL_WIDTH = float(X[3]),
	SPECIES = X[4]
	))

irisDf = SpSession.createDataFrame(irisMap)
####
#Add a numeric indexer for the label/target column:
from pyspark.ml.feature import StringIndexer

#Specify the feature you want to transform:
stringIndexer = StringIndexer(inputCol = 'SPECIES', outputCol = 'IND_SPECIES')
si_model = stringIndexer.fit(irisDf)
irisNormDf = si_model.transform(irisDf)
irisNormDf.show()

irisNormDf.select('SPECIES', 'IND_SPECIES').distinct().collect()
irisNormDf.cache()

irisNormDf.describe().show()


#Perform data analysis:
for i in irisNormDf.columns:
	if not(isinstance(irisNormDf.select(i).take(1)[0][0], str)):
		print('Correlation to Species for', i, irisNormDf.stat.corr('IND_SPECIES', i))


#Drop the columns that are not required:
from pyspark.ml.linalg import Vectors

def transformToLabeledPoint(row):
	lp = (row['SPECIES'], row['IND_SPECIES'],
		Vectors.dense(row['SEPAL_LENGTH'],
			row['SEPAL_WIDTH'],
			row['PETAL_LENGTH'],
			row['PETAL_WIDTH']))
	return lp

irislp = irisNormDf.rdd.map(transformToLabeledPoint)
irisLpDf = SpSession.createDataFrame(irislp, ["species", "label", "features"])
irisLpDf.select('label','features', 'species').show(10)
irisLpDf.cache()

#Split the training data:

(trainingData, testData) = irisLpDf.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()
testData.collect()

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

dt_classifier = DecisionTreeClassifier(maxDepth = 2, labelCol = 'label',
	featuresCol = 'features')
dt_model = dt_classifier.fit(trainingData)
dt_model.numNodes
dt_model.depth

######
predictions = dt_model.transform(trainingData)
predictions.select('prediction', 'species', 'label').collect()

####
evaluator = MulticlassClassificationEvaluator(predictionCol = 'prediction',
	labelCol = 'label', metricName = 'accuracy')
evaluator.evaluate(predictions)

cv_predict = dt_model.transform(testData)
evaluator.evaluate(cv_predict)

#####
predictions.groupBy('label', 'prediction').count().show()



#####
#Principal Component Analysis(PCA) and Random Forest:

bankData = SpContext.textFile('bank.csv')
bankData.cache()
bankData.count()

###
firstline = bankData.first()
datalines = bankData.filter(lambda X: X != firstline)

import math
from pyspark.ml.linalg import Vectors

def transformToNumeric( inputStr) :
    
    attList=inputStr.replace("\"","").split(";")
    
    age=float(attList[0])
    #convert outcome to float    
    outcome = 0.0 if attList[16] == "no" else 1.0
    
    #create indicator variables for single/married    
    single= 1.0 if attList[2] == "single" else 0.0
    married = 1.0 if attList[2] == "married" else 0.0
    divorced = 1.0 if attList[2] == "divorced" else 0.0
    
    #create indicator variables for education
    primary = 1.0 if attList[3] == "primary" else 0.0
    secondary = 1.0 if attList[3] == "secondary" else 0.0
    tertiary = 1.0 if attList[3] == "tertiary" else 0.0
    
    #convert default to float
    default= 0.0 if attList[4] == "no" else 1.0
    #convert balance amount to float
    balance=float(attList[5])
    #convert loan to float
    loan= 0.0 if attList[7] == "no" else 1.0
    
    #Create a row with cleaned up and converted data
    values= Row(     OUTCOME=outcome ,\
                    AGE=age, \
                    SINGLE=single, \
                    MARRIED=married, \
                    DIVORCED=divorced, \
                    PRIMARY=primary, \
                    SECONDARY=secondary, \
                    TERTIARY=tertiary, \
                    DEFAULT=default, \
                    BALANCE=balance, \
                    LOAN=loan                    
                    ) 
    return values

bankRows = datalines.map(transformToNumeric)
bankRows.collect()[:5]

#####
bankData = SpSession.createDataFrame(bankRows)

for i in bankData.columns:
	if not (isinstance(bankData.select(i).take(1)[0][0], str)):
		print('Correlation to Outcome for ', i, bankData.stat.corr('OUTCOME', i))

#Transform to a dataframe:
def transformToLabeledPoint(row) :
    lp = ( row["OUTCOME"], \
            Vectors.dense([
                row["AGE"], \
                row["BALANCE"], \
                row["DEFAULT"], \
                row["DIVORCED"], \
                row["LOAN"], \
                row["MARRIED"], \
                row["PRIMARY"], \
                row["SECONDARY"], \
                row["SINGLE"], \
                row["TERTIARY"]
        ]))
    return lp
    
bankLp = bankData.rdd.map(transformToLabeledPoint)
bankLp.collect()
bankDF = SpSession.createDataFrame(bankLp,["label", "features"])
bankDF.select("label","features").show(10)

"""--------------------------------------------------------------------------
Perform Machine Learning
-------------------------------------------------------------------------"""

#Perform PCA
from pyspark.ml.feature import PCA
bankPCA = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
pcaModel = bankPCA.fit(bankDF)
pcaResult = pcaModel.transform(bankDF).select("label","pcaFeatures")
pcaResult.show(truncate=False)


#Indexing:
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol = 'label', outputCol = 'indexed')
si_model = stringIndexer.fit(pcaResult)
td = si_model.transform(pcaResult)
td.collect()


(trainingData, testData) = td.randomSplit([0.8, 0.2])
trainingData.count()
testData.count()

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

RF_classifier = RandomForestClassifier(labelCol = 'indexed',
	featuresCol = 'pcaFeatures')
rf_model = RF_classifier.fit(trainingData)

####
predictions = rf_model.transform(testData)
predictions.select('prediction', 'indexed', 'label', 'pcaFeatures').collect()
evaluator = MulticlassClassificationEvaluator(predictionCol = 'prediction',
	labelCol = 'indexed', metricName = 'accuracy')
evaluator.evaluate(predictions)
evaluator.evaluate(rf_model.transform(trainingData))

predictions.groupBy('indexed', 'prediction').count().show()


################
#Navie bayes:
smsData = SpContext.textFile('SMSSpamCollection.csv', 2)
smsData.cache()
smsData.collect()

#transform to vector:
def TransformToVector(inputStr):
	attlist = inputStr.split(",")
	smsType = 0.0 if attlist[0] == 'ham' else 1.0
	return [smsType, attlist[1]]

smsXformed = smsData.map(TransformToVector)
smsDf = SpSession.createDataFrame(smsXformed, ["label", "message"])
smsDf.cache()
smsDf.select("label", "message").show()


(trainingData, testData) = smsDf.randomSplit([0.9, 0.1])

from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.feature import IDF, HashingTF, Tokenizer
from pyspark.ml import Pipeline

tokenizer = Tokenizer(inputCol = 'message', outputCol = 'words')
hashingTF = HashingTF(inputCol = tokenizer.getOutputCol(), outputCol = 'tempfeatures')
idf = IDF(inputCol = hashingTF.getOutputCol(), outputCol = 'features')
nb_classifier = NaiveBayes()

pipeline = Pipeline(stages = [tokenizer, hashingTF, idf, nb_classifier])

nb_model = pipeline.fit(trainingData)
prediction = nb_model.transform(testData)

evaluator = MulticlassClassificationEvaluator(predictionCol = 'prediction',
	labelCol = 'label', metricName = 'accuracy')
evaluator.evaluate(prediction)
evaluator.evaluate(nb_model.transform(trainingData))

prediction.groupBy('label', 'prediction').count().show()
prediction2 = nb_model.transform(trainingData)
prediction2.groupBy('label', 'prediction').count().show()


#Kmeans:
autoData = SpContext.textFile('auto-data.csv')
autoData.cache()

####
firstline = autoData.first()
datalines = autoData.filter(lambda X: X != firstline)
datalines.count()

from pyspark.sql import Row

import math
def transformToNumeric( inputStr) :
    attList=inputStr.split(",")
    doors = 1.0 if attList[3] =="two" else 2.0
    body = 1.0 if attList[4] == "sedan" else 2.0  
    #Filter out columns not wanted at this stage
    values= Row(DOORS= doors, \
                     BODY=float(body),  \
                     HP=float(attList[7]),  \
                     RPM=float(attList[8]),  \
                     MPG=float(attList[9])  \
                     )
    return values

autoMap = dataLines.map(transformToNumeric)
autoMap.persist()
autoMap.collect()

autoDf = SpSession.createDataFrame(autoMap)
autoDf.show()

#Centering and scaling. To perform this every value should be subtracted
#from that column's mean and divided by its Std. Deviation.

summStats=autoDf.describe().toPandas()
meanValues=summStats.iloc[1,1:5].values.tolist()
stdValues=summStats.iloc[2,1:5].values.tolist()

#place the means and std.dev values in a broadcast variable
bcMeans=SpContext.broadcast(meanValues)
bcStdDev=SpContext.broadcast(stdValues)

def centerAndScale(inRow) :
    global bcMeans
    global bcStdDev
    meanArray=bcMeans.value
    stdArray=bcStdDev.value
    retArray=[]
    for i in range(len(meanArray)):
        retArray.append( (float(inRow[i]) - float(meanArray[i])) /\
            float(stdArray[i]) )
    return Vectors.dense(retArray)
    
csAuto = autoDf.rdd.map(centerAndScale)
csAuto.collect()

#Create a Spark Data Frame
autoRows=csAuto.map( lambda f:Row(features=f))
autoDf = SpSession.createDataFrame(autoRows)

autoDf.select("features").show(10)

from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(autoDf)
predictions = model.transform(autoDf)
predictions.show()

import pandas as pd


#Process each of the sample:
def unstripData(instr):
	return(instr['prediction'], instr['features'][0],
		instr['features'][1], instr['features'][2], instr['features'][3])

unstripped = predictions.rdd.map(unstripData)
predList = unstripped.collect()
predPd = pd.DataFrame(predList)

import matplotlib.pyplot as plt 
plt.cla()
plt.scatter(predPd[3], predPd[4], c = predPd[0])

#############
#Recommendation Engine:
ratingsData = SpContext.textFile('UserItemData.txt')
ratingsData.collect()

#Convert the strings into vector:
ratingVector = ratingsData.map(lambda l: l.split(',')).map(lambda l: (int(l[0]), int(l[1]), float(l[2])))
ratingsDf = SpSession.createDataFrame(ratingVector, ['user', 'item', 'rating'])

#####
from pyspark.ml.recommendation import ALS
als = ALS(rank = 8, maxIter = 20)
model = als.fit(ratingsDf)
model.userFactors.orderBy('id').collect()

####
#Create test data:
testDf = SpSession.createDataFrame([(1001, 9003), (1001, 9004), (1001, 9005)], ['user', 'item'])

predictions = model.transform(testDf).collect()
predictions


