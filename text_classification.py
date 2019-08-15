#Simple Text classification via sparkml:

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
import pyspark.sql.types as T 
import pyspark.sql.functions as F
import pyspark
import os
import pandas ps pd
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
spark = SparkSession \
.builder.appName('Toxic Comment Classification')
                  .enableHiveSupport()
                  .config("spark.executor.memory", "4G")
                  .config("spark.driver.memory","18G")
                  .config("spark.executor.cores","7")
                  .config("spark.python.worker.memory","4G")
                  .config("spark.driver.maxResultSize","0")
                  .config("spark.sql.crossJoin.enabled", "true")
                  .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                  .config("spark.default.parallelism","2").getOrCreate()

train_df = pd.read_csv('trains.csv')
train_df.fillna('', inplace = True)
train_df = spark.createDataFrame(train_df)
test_df = pd.read_csv('test.csv')
test_df.fillna('', inplace = True)
test_df = spark.createDataFrame(test_df)


out_cols = [i for i in train_df.columns if i not in ['id', 'comment_text']]
#
train_df.filter(F.col('toxic') == 1).show(5)

#Tokenizer:
tokenizer = Tokenizer(inputCol = 'comment_text', outputCol = 'words')
words_data = tokenizer.transform(train_df)
#
hashing_tf = HashingTF(inputCol = 'words', outputCol = 'rawFeatures')
tf = hashing_tf.transform(words_data)

tf.select('rawFeatures').take(5)

tf.count(), len(tf.columns)

idf = IDF(inputCol = 'rawFeatures', outputCol = 'features')
idfModel = idf.fit(tf)
tf_idf = idfModel.transform(tf)

####
#Performing the logistic regression:
REG = 0.01
lr = LogisticRegression(featuresCol = 'features', labelCol = 'toxic',
	regParam = REG)
lr_model = lr.fit(tf_idf)
res_train = lr_model.transform(tf_idf)
res_train.select('id', 'toxic', 'probability', 'prediction').show(10)
#####
extract_prob = F.udf(lambda X: float(X[1]), T.FloatType())

(res_train.withColumn('proba', extract_prob('probability')).select('proba', 'prediction').show())

#Create the results dataframe:
test_tokens = tokenizer.transform(test_df)
test_tf = hashing_tf.transform(test_tokens)
test_tfidf = idfModel.transform(test_tf)

#Output the prob:
test_res = test_df.select('id')
for col in out_cols:
	print(col)
	lr = LogisticRegression(featuresCol = 'features', labelCol = col, regParam = 0.001)
	lr_model = lr.fit(tf_idf)
	res = lr_model.transform(test_tfidf)
	test_res = test_res.join(res.select('id', 'probability'), on = 'id')
	test_res = test_res.withColumn(col, extract_prob('probability')).drop('probability')
	test_res.show(5)

#Visualize the test result:
test_res.show(10)

