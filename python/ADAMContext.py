from pyspark.rdd import RDD
from pyspark.seralizers import MUTF8Deserializer

class ADAMContext():
    def __init__(self, context):
        self._sc = context

    def loadAsJSON(self, name):
        return RDD(self._sc._jsc.parquetFileAsJSON(name), self._sc, MUTF8Deserializer())