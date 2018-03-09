package com.ppg.util

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil


/**
  * Created by 491620 on 1/30/2016.
  */

class FileHandler(df:DataFrame){

  def frmt(fieldDelimiter: String): RDD[String] = {
    val fmttarget = df.rdd.map(row => row.mkString(fieldDelimiter))
    fmttarget
  }

  /* Spark2.0 we will be using DataFrame for save operation*/
  def saveOverWriteDF(outputFile: String, config: Config): Unit = {

    val fieldDelimiter = config.getString("FieldDelimiter")
    val parquetFileSuffix = config.getString("parquetFileSuffix")
    val textFileSuffix = config.getString("textFileSuffix")
    val saveParquetFormat = config.getBoolean("saveParquetFormat")
    val saveTextFormat = config.getBoolean("saveTextFormat")
    val deleteFile = config.getBoolean("deleteFile")

    //The reason this causes a problem is that we're reading and writing to the same path that we're trying to overwrite.
    //This causes an issue since the data cannot be stream into the same directory we're attempting to overwrite.
    //I'd recommend adding a temporary location to do rewrite this data.
    if (saveParquetFormat) {
      if (deleteFile)
      HadoopUtil.delete(outputFile + parquetFileSuffix)
      df.write.format("parquet").mode("overwrite").parquet(outputFile + parquetFileSuffix)
    }

    if (saveTextFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + textFileSuffix)
      frmt(fieldDelimiter).saveAsTextFile(outputFile + textFileSuffix)
    }
  }

  /* Spark1.3 we will be using DataFrame for save operation*/
  def saveDF(outputFile: String, config: Config): Unit = {

    val fieldDelimiter = config.getString("FieldDelimiter")
    val parquetFileSuffix = config.getString("parquetFileSuffix")
    val textFileSuffix = config.getString("textFileSuffix")
    val saveParquetFormat = config.getBoolean("saveParquetFormat")
    val saveTextFormat = config.getBoolean("saveTextFormat")
    val deleteFile = config.getBoolean("deleteFile")

    if (saveParquetFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + parquetFileSuffix)
      df.write.mode("overwrite").parquet(outputFile + parquetFileSuffix)
    }

    if (saveTextFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + textFileSuffix)
      frmt(fieldDelimiter).saveAsTextFile(outputFile + textFileSuffix)
    }
  }

  /* Spark1.3 we will be using DataFrame for save operation*/
  def saveDFAsPQ(outputFile: String, config: Config): Unit = {

    val parquetFileSuffix = config.getString("parquetFileSuffix")
    val saveParquetFormat = config.getBoolean("saveParquetFormat")
    val deleteFile = config.getBoolean("deleteFile")

    if (saveParquetFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + parquetFileSuffix)
      df.write.format("parquet").mode("overwrite").parquet(outputFile + parquetFileSuffix)
    }
  }
  
  def saveAppendPQ(outputFile: String, config: Config): Unit = {

    val parquetFileSuffix = config.getString("parquetFileSuffix")
    val saveParquetFormat = config.getBoolean("saveParquetFormat")
    val deleteFile = config.getBoolean("deleteFile")

    if (saveParquetFormat) {
      //if (deleteFile) HadoopUtil.delete(outputFile + parquetFileSuffix)
      df.write.format("parquet").mode("append").parquet(outputFile + parquetFileSuffix)
    }
  }
  
  def saveDFAsORC(outputFile: String, config: Config): Unit = {

    val fieldDelimiter = config.getString("FieldDelimiter")
    val orcFileSuffix = config.getString("orcFileSuffix")
    val textFileSuffix = config.getString("textFileSuffix")
    val saveOrcFormat = config.getBoolean("saveParquetFormat")
    val saveTextFormat = config.getBoolean("saveTextFormat")
    val deleteFile = config.getBoolean("deleteFile")

    if (saveOrcFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + orcFileSuffix)
      df.write.mode("overwrite").orc(outputFile)
    }

    if (saveTextFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + textFileSuffix)
      frmt(fieldDelimiter).saveAsTextFile(outputFile + textFileSuffix)
    }
  }
  
  
  def saveDFAsTextOverwrite(outputFile: String, config: Config): Unit = {

    val fieldDelimiter = config.getString("FieldDelimiter")
    val orcFileSuffix = config.getString("orcFileSuffix")
    val textFileSuffix = config.getString("textFileSuffix")
    val saveOrcFormat = config.getBoolean("saveParquetFormat")
    val saveTextFormat = config.getBoolean("saveTextFormat")
    val deleteFile = config.getBoolean("deleteFile")

      frmt(fieldDelimiter).saveAsTextFile(outputFile + textFileSuffix)
  }
    
  
  def saveAppendORC(outputFile: String, config: Config): Unit = {

    val saveOrcFormat = config.getBoolean("saveParquetFormat")
    val orcFileSuffix = config.getString("orcFileSuffix")
    val deleteFile = config.getBoolean("deleteFile")
    val saveTextFormat = config.getBoolean("saveTextFormat")
    val fieldDelimiter = config.getString("FieldDelimiter")
    val textFileSuffix = config.getString("textFileSuffix")

    if (saveOrcFormat) {
      //if (deleteFile) HadoopUtil.delete(outputFile + parquetFileSuffix)
      df.write.mode("append").orc(outputFile)
    }
    if (saveTextFormat) {
      frmt(fieldDelimiter).saveAsTextFile(outputFile + textFileSuffix)
    }
  }

  /**
    * @desc this function is created for cases where only specific files need to be saved as text file, for instance, the final output alone needs to be saved as
    * text. This above function provides a toggle point which will impact all intermediate output files as well. Hence this additional function
    * @note this method will also save the dataframe in  parquet format
    */
  def saveDFAsPQText(outputFile: String, config: Config): Unit = {

    val fieldDelimiter = config.getString("FieldDelimiter")
    val parquetFileSuffix = config.getString("parquetFileSuffix")
    val textFileSuffix = config.getString("textFileSuffix")
    val deleteFile = config.getBoolean("deleteFile")

    if (deleteFile) HadoopUtil.delete(outputFile + parquetFileSuffix)
    df.write.save(outputFile + parquetFileSuffix)

    if (deleteFile) HadoopUtil.delete(outputFile + textFileSuffix)
    frmt(fieldDelimiter).saveAsTextFile(outputFile + textFileSuffix)
  }



}

object FileHandler {
  implicit def persist(df:DataFrame)= new FileHandler(df)
  def saveSRDDasFiles(schemaRDD: DataFrame, outputFile: String, config: Config): Unit = {

    val fieldDelimiter = config.getString("FieldDelimiter")
    val parquetFileSuffix = config.getString("parquetFileSuffix")
    val textFileSuffix = config.getString("textFileSuffix")
    val saveParquetFormat = config.getBoolean("saveParquetFormat")
    val saveTextFormat = config.getBoolean("saveTextFormat")
    val deleteFile = config.getBoolean("deleteFile")


    if (saveParquetFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + parquetFileSuffix)
      schemaRDD.write.save(outputFile + parquetFileSuffix)
    }

    if (saveTextFormat) {
      if (deleteFile) HadoopUtil.delete(outputFile + textFileSuffix)
      frmt(schemaRDD, fieldDelimiter).saveAsTextFile(outputFile + textFileSuffix)
    }
  }



  def frmt(tgt: DataFrame, fieldDelimiter: String): RDD[String] = {
    val fmttarget = tgt.rdd.map(row => row.mkString(fieldDelimiter))
    fmttarget
  }

  def frmtRDD(tgt: RDD[org.apache.spark.sql.Row], fieldDelimiter: String): RDD[String] = {
    val fmttarget = tgt.map(row => row.mkString(fieldDelimiter))
    fmttarget
  }
}

