package org.bdgenomics.adam.python

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext
import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMRecord }

class PythonADAMContext(sc: SparkContext) {

  val loader = new ParquetContext(sc)

  def loadADAMRecords(path: String): JavaRDD[Array[Byte]] = {
    loader.parquetFileAsSerializedAvro[ADAMRecord](path)
  }

  def loadADAMGenotypes(path: String): JavaRDD[Array[Byte]] = {
    loader.parquetFileAsSerializedAvro[ADAMGenotype](path)
  }
}
