#pyspark:
import pyspark.sql.functions as F
from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName("Python Spark Example").getOrCreate()
#config("spark.some.config.option", "some-value").getOrCreate()

df = spark.sparkContext.parallelize([(1, 2, 3, 'a b c'),
	(4, 5, 6, 'd e f'),
	(7, 8, 9, 'g h i')]).toDF(['col1', 'col2', 'col3', 'col4'])

df.show()

#RDD object: construct a DAG 有向无环图.
#First build operator DAG
#secondly split graph into stages of tasks and then submit each stage as ready.
#thirdly: launch tasks via cluster manager and threads/ block managers implement works.

Employee = spark.createDataFrame([
('1', 'Joe', '70000', '1'),
('2', 'Henry', '80000', '2'),
('3', 'Sam', '60000', '2'),
('4', 'Max', '90000', '1')],
['Id', 'Name', 'Sallary','DepartmentId']
)
Employee.show(5)
Employee.printSchema()


my_list = [('a', 2, 3),
('b', 5, 6),
('c', 8, 9),
('a', 2, 3),
('c', 8, 9)]


col_name = ['col1', 'col2', 'col3']

dp = pd.DataFrame(my_list, columns = col_name)
ds = spark.createDataFrame(my_list, schema = col_name)

dp['concat'] = dp.apply(lambda X: '%s%s'%(x['col1'], x['col2']), axis = 1)
dp

dp.withColumn('concat', F.concat('col1', 'col2')).show()

dp.groupby(['col1']).agg({'col2': 'min', 'col3': 'mean'})



#Linear regression:
from pyspark.sql import SparkSession

df = spark.read.format('com.databricks.spark.csv').options(header = 'true', inferschema = 'true').load('/Users/ruanhq/Desktop/Suning/Preliminary_Materials/data/Advertising.csv', header = True)
df.show(5, True)
df.printSchema()
df.describe().show()

from pyspark.sql import Row 
from pyspark.ml.linalg import Vectors 

##
def transData(data):
	return data.rdd.map(lambda r: [Vectors.dense(r[:-1]), r[-1]]).toDF(['features',
		'label'])


def get_dummy(df, indexCol, categoricalCols, continuousCols, labelCol):
	from pyspark.ml import Pipeline 
	from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler 
	from pyspark.sql.functions import col 
	indexers = [StringIndexer(inputCol = c, outputCol = "{0}_indexed").format(c)
	for c in categoricalCols]
	encoders = [OneHotEncoder(inputCols = indexer.getOutputCol(), outputCol = "{0}_encoded".format(indexer.getOutputCol())) for indexer in indexers ]
	#
	assembler = VectorAssembler(inputCols = [encoder.getOutputCol() for encoder in encoders] + continuousCols, outputCol = "features")
	#
	pipeline = Pipeline(stages = indexers + encoders + [assembler])
	model = pipeline.fit(df)
	data = model.transform(df)
	data = data.withColumn('label', col(labelCol))
	return data.select(indexCol, 'features', 'label')

transformed = transData(df)
transformed.show(5)

from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

feature_indexer = VectorIndexer(inputCol = "features", outputCol = "indexedFeatures",
	maxCategories = 4).fit(transformed)

data = feature_indexer.transform(transformed)
data.show(5)

(trainingData, testData) = transformed.randomSplit([0.6, 0.4])
trainingData.show(5)
testData.show(5)

from pyspark.ml.regression import LinearRegression 
lr = LinearRegression()

pipeline = Pipeline(stages = [feature_indexer, lr])
model = pipeline.fit(trainingData)

summary_1 = model.stages[-1].summary
# p_value: [coef[i]]

import numpy as np 
def modelsummary(model):
	coef = np.append(list(model.coefficients), model.intercept)




#1. Implemented two single-cell trajectory inference algorithms and modified one of them to adapt for biological nature for scRNA-seq data. Conducted simulations investigating its efficiency, achieved improved accuracy and robustness.
#2. Developed a penalized linear model to investigate cell-cell interactions in scRNA-seq data, achieved improved efficiency in terms of the identification of marker genes potentially related to the interactions.
#3. Developed a nested generative model to deconvolve mixed cell populations into their individual components and identify biomarkers related to co-variation in normal cells and tumor cells. Achieved improved correlation with ground truth relative contribution of each cell population on top of current methods(NNLS).
#4. Collaborated with biologists and clinicians from Stanford University School of Medicine designing and implementing data analysis pipeline for single-cell genomic data. Successfully identified 2 genes related to high-disease survival, 3 genes related to neurodegenerative diseases and 2 genes related to apoptosis. The poster demonstrated the findings appeared in a top conference.


