package org.bdgenomics.adam.python

import org.apache.spark.api.java.JavaRDD
import parquet.avro.AvroReadSupport
import org.apache.avro.generic.GenericRecord
import parquet.hadoop.ParquetInputFormat
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.specific.{ SpecificDatumWriter, SpecificRecord }
import org.apache.avro.io.{ EncoderFactory, BinaryEncoder }
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream
import org.apache.spark.rdd.RDD

class ParquetContext(sc: SparkContext) {

  def parquetFileAsJSON(path: String): JavaRDD[String] = {
    val job = Job.getInsta

  def parquetFileAsAvro[T <: SpecificRecord: Manifest](path: String): JavaRDD[T] = {
    val job = Job.getInstance(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])
    sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[T]], classOf[Void], manifest[T].runtimeClass.asInstanceOf[Class[T]],
      job.getConfiguration).map(_._2)
  }

  def parquetFileAsSerializedAvro[T <: SpecificRecord: Manifest](path: String): JavaRDD[Array[Byte]] = {
    def serialize(record: T): Array[Byte] = {
      val writer = new SpecificDatumWriter[T](manifest[T].runtimeClass.asInstanceOf[Class[T]])
      val outstream = new FastByteArrayOutputStream()
      val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null.asInstanceOf[BinaryEncoder])
      writer.write(record, encoder)
      outstream.array
    }
    val rdd: RDD[T] = parquetFileAsAvro[T](path)
    rdd.map(serialize)
  }

}