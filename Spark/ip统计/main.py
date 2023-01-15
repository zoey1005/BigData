from pyspark.sql import SparkSession


# 将ip转化为特殊的数字形式，二进制左移八位的方式
def ip_transform(ip):
    ips = ip.split('.')
    ip_num = 0
    for i in ips:
        ip_num = int(i) | ip_num << 8
    return ip_num


# 二分法找到ip对应的行的索引
def binary_search(ip_num, broadcast_value):
    start = 0
    end = len(broadcast_value) - 1
    while start <= end:
        mid = int((start + end) / 2)
        if int(broadcast_value[mid][0]) <= ip_num <= int(broadcast_value[mid][1]):
            return mid
        if ip_num < int(broadcast_value[mid][0]):
            end = mid
        if ip_num > int(broadcast_value[mid][1]):
            start = mid


# if __name__ =='__main__':
#       sc = SparkContext('local[2]', 'iptopN')
def main():
    spark = SparkSession.builder.appName('test').getOrCreate()
    sc = spark.sparkContext

    city_id_rdd = sc.textFile('file:///home/hadoop/ip.txt').map(lambda x: x.split('|')). \
        map(lambda x: (x[2], x[3], x[13], x[14]))
    # 创建一个广播变量
    """
    广播变量: 一种只读变量，它会被复制到每一个节点上，以供在分布式集群中的每一个任务使用。
    广播变量能够提高性能，因为它们可以避免在分布式集群中重复传输大量数据。
    """
    city_broadcast = sc.broadcast(city_id_rdd.collect())
    dest_data = sc.textFile('file:///home/hadoop/20090121000132.394251.http.format'). \
        map(lambda x: (x[2], x[3], x[13], x[14]))

    # 根据取出值获取位置信息
    def get_pos(x):
        city_broadcast_value = city_broadcast.value

        # 根据单个ip获取对应经纬信息
        def get_result(ip):
            ip_num = ip_transform(ip)
            index = binary_search(ip_num, city_broadcast_value)
            # ((纬度，经度),1)
            return ((city_broadcast_value[index][2], city_broadcast_value[index][3]), 1)

        x = map(tuple, [get_result(ip) for ip in x])
        return x

    dest_rdd = dest_data.mapPartitions(lambda x: get_pos(x))
    result_rdd = dest_rdd.reduceByPartition(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)
    print(result_rdd.collect())
    sc.stop()
    """
    mapPartitions：
    transformation操作
    将RDD的每个分区映射为另一个RDD的分区。     
    该函数接受一个参数，即将被映射的分区的迭代器，并返回一个新的迭代器。
    在运行mapPartitions时，所有分区的数据将被加载到内存中，并且对于每个分区都会调用一次提供的函数，并返回新的迭代器。
    这可以减少数据的序列化和反序列化的开销。
    """
