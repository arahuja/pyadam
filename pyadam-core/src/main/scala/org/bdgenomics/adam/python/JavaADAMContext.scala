package org.bdgenomics.adam.python

import org.apache.spark.api.java.{ JavaSparkContext, JavaRDD }
import parquet.avro.AvroReadSupport
import org.apache.avro.generic.GenericRecord
import parquet.hadoop.ParquetInputFormat
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.mapreduce.Job
import org.apache.avro.specific.SpecificRecord

//class JavaADAMContext(sc: SparkContext) extends JavaSparkContext(sc: SparkContext) {
//
//  def this(conf: SparkConf) = this(new SparkContext(conf))
//
//  def parquetFileAsJSON(path: String): JavaRDD[String] = {
//    new JavaRDD(new PythonADAMContext(sc).parquetFileAsJSON(path, sc.hadoopConfiguration))
//  }
//
//  def parquetFileAsAvro(path: String): JavaRDD[SpecificRecord] = {
//    new JavaRDD(new PythonADAMContext(sc).parquetFileAsAvro(path, sc.hadoopConfiguration))
//  }
//
//}

class ParquetContext(sc: SparkContext) { //extends JavaSparkContext(sc: SparkContext) {

  //def this(conf: SparkConf) = this(new SparkContext(conf))

  def parquetFileAsJSON(path: String): JavaRDD[String] = {
    //new JavaRDD(new PythonADAMContext(sc).parquetFileAsJSON(path, sc.hadoopConfiguration))
    val job = Job.getInstance(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[GenericRecord]])
    sc.newAPIHadoopFile(path, classOf[ParquetInputFormat[GenericRecord]], classOf[Void], classOf[GenericRecord],
      job.getConfiguration).map(pair => pair._2.toString)
  }

  def parquetFileAsAvro(path: String): JavaRDD[SpecificRecord] = {
    new JavaRDD(new PythonADAMContext(sc).parquetFileAsAvro(path, sc.hadoopConfiguration))
  }

}