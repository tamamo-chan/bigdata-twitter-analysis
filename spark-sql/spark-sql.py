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


sentimentDF = sqlContext.read.json("sentiment.json")
#sentimentDF.show()

#x =  sqlContext.sql("select date from cases where date > 20201212").collect()
#y =  sqlContext.sql("select positiveIncrease from cases where date > 20201212").collect

#plt.bar(x,y)

#fig = plt.figure()
#fig.savefig('figure.png')

                                                                                                                                                     
covDF.registerTempTable("cases")
covRDD = sqlContext.sql("select date, positiveIncrease from cases where date > 20201212 order by date asc")
covPandas = covRDD.toPandas()
sentimentPandas = sentimentDF.toPandas()                                                                                                                                                     
covPlot = covPandas.plot(kind='barh',x='date',y='positiveIncrease',colormap='winter_r')
tweetsPlot = sentimentPandas.plot(kind='barh',x='date',y='tweets',colormap='winter_r')
sentimentPlot = sentimentPandas.plot(kind='barh',x='date',y='positiveSentiment',colormap='winter_r')

fig1 = covPlot.get_figure()
fig1.savefig('figure1.png')
fig2 = tweetsPlot.get_figure()
fig2.savefig('figure2.png')
fig3 = sentimentPlot.get_figure()
fig3.savefig('figure3.png')
