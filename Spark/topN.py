from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('topN').getOrCreate()
sc = spark.sparkContext
rdd1 = sc.textFile('file:///root/bigdata/data/access.log')
# 对每一行按空格分开，把ur1记为1
rdd2 = rdd1.map(lambda x:x.split(" ")).filter(lambda x:len(x)>10).map(lambda x:(x[10],1))
# 对数据进行累加，按出现次数降序排列
rdd3 = rdd2.reduceByKey(lambda a,b:a+b).sortBy(lambda x:x[1],ascending = False)
rdd4 = rdd3.tabke(3)
rdd4.collect()
sc.stop()
