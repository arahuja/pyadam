from pyspark import SparkContext
from pyspark.serializers import MUTF8Deserializer
from pyspark.rdd import RDD

try:
	import avro.protocol as apr
except:
	pass

import json

from parquet_context import ParquetContext
from avroserde import AvroDeserializer

class ADAMContext(object):

	def __init__(self, sc):
		# Set the PySpark context
		self._sc = sc
		
		# Load the ADAM avro record schema
		self._adam_record_schema = apr.parse(open("adam.avpr", "r").read()) 
		
		# Set the JavaSparkContext
		self._jsc = sc._jsc
		
		# Store the parquet context which as a loader
		self._pc = self._sc._jvm.PythonADAMContext(self._sc._jsc.sc())


	def loadADAMRecords(self, path):
		return RDD(self._pc.loadADAMRecords(path), self._sc, AvroDeserializer(self._adam_record_schema))
	
	""" 
		This will nicely override the _initialize_context parent function to return
		to get an ADAMContext, BUT! not this version...
	"""
	def _initialize_context(self, jconf):
		return self._jvm.JavaADAMContext(self._jsc.sc())
