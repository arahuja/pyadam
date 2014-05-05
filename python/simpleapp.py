from pyspark import SparkConf, SparkContext
from py4j.java_gateway import java_import

from java_gateway_contrib import launch_gateway
from parquet import ParquetContext

## TODO
## from adam_spark_context import ADAMContext

if __name__ == '__main__':
	
	"""Launch my OWN gateway"""
	## You can pass a gateway to SparkContext._ensure_initialized
	## in the latest pyspark ...
	SparkContext._gateway = launch_gateway()
	SparkContext._jvm = SparkContext._gateway.jvm
	SparkContext._writeToFile = SparkContext._jvm.PythonRDD.writeToFile

	conf = SparkConf()
	
	# Set master
	conf.setMaster("local[8]")

	conf.setAppName("pyADAM")
	conf.set("spark.cores.max", "4")
	conf.set("spark.executor.memory", "16g")

	""" Try and change spark context to be aware of new jar"""
	## Adding jar
	conf.set("spark.jars", ADAM_JAR)
	sc = SparkContext(conf = conf, pyFiles = ['simpleapp.py'])

	""" Try creating a new context thing that may be useful """
	parquet_loader = ParquetContext( sc )

	""" Try and make RDD of stuff """
	rdd = parquet_loader.load(SMALL_PARQUET)
	print "Records in RDD:", adam_rdd.count()

	print "Records sample..."
	print adam_rdd.map(lambda x: (x['readName'], x['sequence'], x['cigar'])).take(10)
