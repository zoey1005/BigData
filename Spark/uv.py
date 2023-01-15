from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pv').getOrCreate()
sc = spark.sparkContext
rdd1 = sc.textFile('file:///root/bigdata/data/access.log')
# 按空格拆分每一行，将ip取出
rdd2 = rdd1.map(lambda x:x.split(" ")).map(lambda x:x[0])
# 把每个ur记为1
rdd3 = rdd2.distinct().map(lambda x:('uv',1))
rdd4 = rdd3.reduceByKey(lambda a,b:a+b)
rdd4.saveAsTextFile('hdfs:///uv/result')
sc.stop()