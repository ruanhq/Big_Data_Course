######


###########
########################################
#Starting code for setting up the environment.
import pyspark
import os
import sys
os.chdir('/Users/ruanhq/Desktop/Davis/Jobs')
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

####
cred_data = SpContext.textFile('credit-card-default-1000.csv')
cred_data.take(5)


####
#Remove the header:
cred_raw = cred_data.filter(lambda X: 'CUSTID' not in X)
cred_raw.count()

###
#Clean the data where the last two rows are obviously wrong.
datalines = cred_raw.filter(lambda X: X.find('aaaaaaaaa') == -1)
datalines.count()


####
from pyspark.sql import Row
import numpy as np



#######
def convert_rows(inputStr):
	attlist = inputStr.split(',')
	#create a new age variable rounded off to 10s:
	age_round = round(float(attlist[5])/ 10) * 10
	#set the sex:
	sex = float(attlist[2].replace('M', '1').replace('F', '2'))
	#Compute average billed amount:
	avg_bill_amount = round(np.array(attlist[12:18], dtype = 'float').mean().item(), 3)
	avg_pay_amount = round(np.array(attlist[18:24], dtype = 'float').mean().item(), 3)
	avg_pay_duration = round((abs(float(attlist[6])) + abs(float(attlist[7])) +
		abs(float(attlist[8])) + abs(float(attlist[9])) + abs(float(attlist[10]))
		+ abs(float(attlist[11])))/6)
	#avg_pay_duration = round(np.array([X if float(X) > 0 else 0 for X in attlist[6:12]], dtype = 'float').mean().item())
	pay_percent = round((avg_pay_amount / (avg_bill_amount + 1) * 100) / 25) * 25
	#return values:
	values = Row(CUSTID = attlist[0], LIMIT_BAL = float(attlist[1]),
		SEX = sex, EDUCATION = float(attlist[3]),
		MARRIAGE = float(attlist[4]), age = age_round,
		AVG_PAY_DUR = avg_pay_duration,
		AVG_BIL_AMT = avg_bill_amount,
		AVG_PAY_AMT = avg_pay_amount,
		PER_PAID = pay_percent,
		DEFAULTED = float(attlist[24]))
	return values

cred_df = SpSession.createDataFrame(datalines.map(convert_rows))
#####
#Add a new column SEXNAME:
genderDf = SpSession.createDataFrame(pd.DataFrame({'SEX': [1.0, 2.0],
	'SEX_NAME': ['Male', 'Female']}))

cred_df1 = cred_df.join(genderDf, cred_df.SEX == genderDf.SEX).drop(genderDf.SEX)
cred_df.take(2)

#####
#Add a new column EDUCATION:
educationDf = SpSession.createDataFrame(pd.DataFrame({'EDUCATION':[1.0, 2.0, 3.0, 4.0],
	'ED_STR':['Graduate', 'University', 'High School', 'Others']}))

cred_df2 = cred_df1.join(educationDf, cred_df1.EDUCATION == educationDf.EDUCATION).drop(educationDf.EDUCATION)
cred_df2.show(5)

#####
#Add a new column containing description for maritus status:
mar_df = SpSession.createDataFrame(pd.DataFrame({'MARRIAGE': [1.0, 2.0, 3.0],
	'MARR_DESC': ['Single', 'Married', 'Others']}))
cred_df3 = cred_df2.join(mar_df, cred_df2.MARRIAGE == mar_df.MARRIAGE).drop(mar_df.MARRIAGE)
cred_df3.show(5)
#####

#Load the dataframe as a temp table view:
cred_df3.createOrReplaceTempView('Cred_Data')

#PR#02:
SpSession.sql('SELECT SEX_NAME, COUNT(*) AS Total, SUM(DEFAULTED) AS Defaults, ROUND(SUM(DEFAULTED) * 100/COUNT(*)) AS PER_DEFAULT FROM Cred_Data GROUP BY SEX_NAME').show()

#PR#03:
SpSession.sql('SELECT MARR_DESC, ED_STR, COUNT(*) AS Total, SUM(DEFAULTED) AS Defaults, ROUND(SUM(DEFAULTED) * 100/COUNT(*)) AS PRE_DEFAULTFROM FROM  Cred_Data GROUP BY MARR_DESC, ED_STR ORDER BY MARR_DESC ASC,  ED_STR ASC').show()

#PR#04:
SpSession.sql('SELECT AVG_PAY_DUR, COUNT(*) AS Total, SUM(DEFAULTED) AS Defaults, ROUND(SUM(DEFAULTED) * 100 / COUNT(*)) AS PER_DEFAULT FROM Cred_Data GROUP BY AVG_PAY_DUR ORDER BY 1'
).show()


####Perform first round correlation analysis:
for i in cred_df3.columns:
	if not(isinstance(cred_df3.select(i).take(1)[0][0], str)):
		print('Correlation to DEFAULTED for', i, cred_df3.stat.corr('DEFAULTED', i))


######
#Prepare the data into the format of ML:
from pyspark.ml.linalg import Vectors

#Transform them to labeled point:
def transformToLabeledPoint(row):
	lp = (
		row['DEFAULTED'],
		Vectors.dense([
			row['age'],
			row['AVG_PAY_DUR'],
			row['AVG_PAY_AMT'],
			row['AVG_BIL_AMT'],
			row['EDUCATION'],
			row['MARRIAGE'],
			row['LIMIT_BAL']
			]))
	return lp

def transformToLabeledPoint(row):
	lp = (
		row['DEFAULTED'],
		Vectors.dense([
			[row['age'],
			row['AVG_PAY_DUR'],....]])
		)

#From dataframe to a formalized data matrix:
cred_lp = cred_df3.rdd.map(transformToLabeledPoint)
cred_lp.take(5)
#Separate the labels and features:
creddf = SpSession.createDataFrame(cred_lp, ['label', 'features'])
creddf.select('*').show(10, truncate = False)
creddf.cache()


###
from pyspark.ml.feature import StringIndexer

stringIndexer = StringIndexer(inputCol = 'label', outputCol = 'indexed')
si_model = stringIndexer.fit(creddf)
td = si_model.transform(creddf)

trainingData, testData = td.randomSplit([0.8, 0.2])
trainingData.count()
testData.count()

from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


evaluator = MulticlassClassificationEvaluator(predictionCol = 'prediction',
	labelCol = 'indexed', metricName = 'accuracy')

paramGrid = ParamGridBuilder().addGrid(DecisionTreeClassifier.maxDepth, [4, 5, 6]).build()
dtClassifier = DecisionTreeClassifier(labelCol = 'indexed',
	featuresCol = 'features')

dtClassifier_cv = CrossValidator(estimator = DecisionTreeClassifier(),
	estimatorParamMaps = paramGrid,
	evaluator = MulticlassClassificationEvaluator(),
	numFolds = 5)

dt_model = dtClassifier_cv.fit(trainingData)
predictions = dt_model.transform(testData)
predictions.select('prediction', 'indexed', 'label', 'features').show()

print('Results of Decision Tree: {}'.format(evaluator.evaluate(predictions)))

trainingData.persist()
rfClassifier = RandomForestClassifier(labelCol = 'indexed',
	featuresCol = 'features'
	)
param_rf = ParamGridBuilder().addGrid(RandomForestClassifier)
param_rf = ParamGridBuilder().addGrid(RandomForestClassifier.maxDepth, [3, 4, 5, 6]).addGrid(RandomForestClassifier.minInstancesPerNode, [3, 5, 7, 9]).build()
rfClassifier_cv = CrossValidator(estimator = RandomForestClassifier(),
	estimatorParamMaps = param_rf,
	evaluator = MulticlassClassificationEvaluator(), numFolds = 5)
rf_model = rfClassifier_cv.fit(trainingData)
prediction_rf = rf_model.transform(testData)
prediction_rf.select('prediction', 'indexed', 'label', 'features').show()

print('Results of Random Forest: {}'.format(evaluator.evaluate(predictions_rf)))

#######
#Gradient boosting trees classifier:
from pyspark.ml.classification import GBTClassifier  
gbt_classifier = GBTClassifier(labelCol = 'indexed', featuresCol = 'features', maxIter = 20)
gbt_model = gbt_classifier.fit(trainingData)
prediction_gbt = gbt_model.transform(testData)
evaluator.evaluate(predictions_gbt)


#######
cred_clus_df = cred_df3.select('SEX', 'EDUCATION', 'MARRIAGE',
	'AGE', 'CUSTID')
summstats = cred_clus_df.describe().toPandas()
summstats

meanValues = summstats.iloc[1, 1:].values.tolist()
stdValues = summstats.iloc[2, 1:].values.tolist()

bcMeans = SpContext.broadcast(meanValues)
bcStdDev = SpContext.broadcast(stdValues)

#round() with no number specified means rounding to the integer.


def centerAndScale(inRow):
    global bcMeans
    global bcStdDev
    
    meanArray = bcMeans.value
    stdArray = bcStdDev.value
    
    retArray = []
    #Scale:
    for i in range(len(meanArray)):
        retArray.append((float(inRow[i]) - float(meanArray[i])) / float(stdArray[i]))
    return Row(CUSTID = inRow[4], features = Vectors.dense(retArray))

def centerAndScale(inRow):
	global bcMeans
	global bcStdDev
	meanArray = bcMeans.value
	stdArray = bcStdDev.value


ccMap = cred_clus_df.rdd.map(centerAndScale)
ccMap.take(5)

###Create a dataframe with the features:
cred_final_clust = SpSession.createDataFrame(ccMap)
cred_final_clust.cache()
cred_final_clust.show(10, truncate = False)

from pyspark.ml.clustering import KMeans
kmeans = KMeans(k = 4, seed = 2019)
km_model = kmeans.fit(cred_final_clust)
prediction_kmeans = km_model.transform(cred_final_clust)
prediction_kmeans.select('*').show()

