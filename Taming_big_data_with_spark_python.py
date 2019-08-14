####
import pyspark
from pyspark import SparkConf, SparkContext


statCharacterID = 5306
targetCharacterID = 14

hitCounter = sc.accumulator(0)


def convertToBFS(line):
	fields = line.split()
	heroID = int(fields[0])
	connections = []
	for connection in fields[1:]:
		connections.append(int(connection))
	####
	color = 'WHITE'
	distance = 9999
	###
	if (heroID == startCharacterID):
		color = 'GRAY'
	distance = 0
	###
	return (heroID, (connections, distance, color))

#####
#RDD: Resilient distributed dataset, immutable, partitioned collection of elements that can be ran in parallel.
#Properties:
#1. List of Partitions.
#2. Function for computing each split
#3. List of dependencies on other RDDs
#4. Optionally, a list of preferred locations to compute each split on:

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import QuantileDiscretizer
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
spark = SparkSession \
.builder \
.master('local[2]')\
.appName('ruanhqspark')\
.config('spark.executor.memory', '1g')\
.config('spark.cores.max', '2')\
.config('spark.sql.warehouse.dir', '/Users/ruanhq/Spark/spark-warehouse').getOrCreate()

SpContext = SpSession.sparkContext

titanic_df = spark.read.csv('train.csv', header = 'True', inferSchema = 'True')
passengers_count = titanic_df.count()
titanic_df.show(5)
titanic_df.describe().show()

#####
#
titanic_df.printSchema()
#
titanic_df.select('Survived', 'Pclass', 'Embarked').show()

titanic_df.groupBy('Survived').count().show()
titanic_df.groupBy('Pclass', 'Survived', 'Embarked').count().show()


#####checking null values:
def null_value_count(df):
	null_columns_counts = []
	numRows = df.count()
	for k in df.columns:
		nullRows = df.where(col(k).isNull()).count()
		if(nullRows > 0):
			temp = k, nullRows
			null_columns_counts.append(temp)
	return(null_columns_counts)

null_value_count(titanic_df)





