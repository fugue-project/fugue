from pyspark import SparkContext, SparkConf
from operator import add

def test_spark():
    sc = SparkContext(conf=(SparkConf().set("spark.master","local[*]")
                                    .set("spark.executor.instances",10)
                                    .set("spark.executor.cores",2)
                                    .set("spark.executor.memory","1g")))
    data = sc.parallelize(list("Hello World"))
    counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()
    for (word, count) in counts:
        print("{}: {}".format(word, count))
        
    # You must manually stop the SparkContext or SparkSession
    # For a K8s cluster if you don't do this manually, you will constantly occupying the resources you requested!
    sc.stop()