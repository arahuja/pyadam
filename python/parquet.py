import json

from pyspark import SparkContext
from pyspark.serializers import PickleSerializer, BatchedSerializer, MUTF8Deserializer
from pyspark.rdd import RDD

try:
	from avroserde import AvroDeserializer
except:
	pass


class ParquetContext(object):
	
	def __init__(self, context):
		self._sc = context
		self._jsc = context._jsc
		
		# Store the parquet context which as a loader
		self._pc = self._sc._jvm.ParquetContext(self._sc._jsc.sc())
	
	def load(self, name, minPartitions = None, type = 'json'):
		return self.loadAsJson(name, minPartitions)
	
	def loadAsAvro(self, name, deserializer, minParitions = None):
		return RDD(self._pc.parquetFileAsAvro(name), self._sc, deserializer)

	def loadAsJson(self, name, minPartitions = None):
		return RDD(self._pc.parquetFileAsJSON(name), self._sc, MUTF8Deserializer()).map(lambda x: json.loads(x))