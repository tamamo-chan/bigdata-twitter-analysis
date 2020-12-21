import os
import pandas
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
os.curdir

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext

conf = SparkConf()
conf.set("spark.executor.memory", "1g")
conf.set("spark.cores.max","2")
conf.setAppName("Twitter sentiment")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

covDF = sqlContext.read.json("result.json")
covDF.registerTempTable("cases")

#x =  sqlContext.sql("select date from cases where date > 20201212").collect()
#y =  sqlContext.sql("select positiveIncrease from cases where date > 20201212").collect

#plt.bar(x,y)

#fig = plt.figure()
#fig.savefig('figure.png')

covDF.registerTempTable("cases")
covRDD = sqlContext.sql("select date, positiveIncrease from cases where date > 20201212")
covPandas = covRDD.toPandas()
plt.hist(covPandas)

fig = plt.figure()
fig.savefig('figure.png')
