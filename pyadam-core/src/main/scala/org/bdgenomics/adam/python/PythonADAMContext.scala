package org.bdgenomics.adam.python

import org.apache.spark.api.java.{ JavaSparkContext, JavaRDD }
import parquet.avro.AvroReadSupport
import org.apache.avro.generic.GenericRecord
import parquet.hadoop.ParquetInputFormat
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.serialization.AvroSerializer
import org.apache.avro.specific.SpecificRecord

//object PythonADAMContext {
//  // Add Python ADAM Spark context methods
//  implicit def sparkContextToPythonADAMContext(sc: SparkContext): PythonADAMContext = new PythonADAMContext(sc)
//
//  def apply(sc: SparkContext): PythonADAMContext = {
//    new PythonADAMContext(sc)
//  }
//
//}

class PythonADAMContext(sc: SparkContext) {

  def parquetFileAsJSON(path: String, conf: Configuration = sc.hadoopConfiguration): RDD[String] = {
    val job = Job.getInstance(conf)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[GenericRecord]])
    sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[GenericRecord]], classOf[Void], classOf[GenericRecord],
      job.getConfiguration).map(pair => pair._2.toString)
  }

  def parquetFileAsAvro(path: String, conf: Configuration = sc.hadoopConfiguration): RDD[SpecificRecord] = {
    val job = Job.getInstance(conf)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[SpecificRecord]])
    sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[SpecificRecord]], classOf[Void], classOf[SpecificRecord],
      job.getConfiguration).map(pair => pair._2)
  }
}

//object JavaADAMContext {
//  // Add Python ADAM Spark context methods
//  implicit def sparkContextToJavaADAMContext(sc: SparkContext): JavaADAMContext = new JavaADAMContext(sc)
//
//  def apply(sc: SparkContext): JavaADAMContext = {
//    new JavaADAMContext(sc)
//  }
//
//}

class PythonADAMKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) = {
    kryo.register(classOf[SpecificRecord], new AvroSerializer[SpecificRecord]())
    kryo.register(classOf[ADAMRecord], new AvroSerializer[ADAMRecord]())
  }
}

