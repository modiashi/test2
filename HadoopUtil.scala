package com.ppg.util

/**
  * Created by 491620 on 1/30/2016.
  */
import java.io.File
import java.net.URI

//import com.proj.util.EpochGenerated.EpochConverter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

object HadoopUtil {

  implicit val conf = SparkHadoopUtil.get.newConfiguration(new SparkConf())

  def delete(path: String) {
    val fs = getHadoopFileSystem(path)
    fs.delete(new Path(path), true)
  }

  def deleteFsFile(path: String): Unit = {
    val fs = getHadoopFileSystem(path)
    fs.delete(new Path(path+ "/"), true)
  }
  def checkPath(path: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = FileSystem.get(new java.net.URI(path), hadoopConf)
    println(fs.toString())
    val status = 
      try{
      fs.listStatus(new Path(path)).filter(!_.getPath.getName.contains("SUCCESS")).length.toInt
    }
    catch {
      case e: Exception => 0
      
    }
    println("Folder status: "+status)
    if (status !=0) true else false
  }

  def getHadoopFileSystem(path: String)(implicit conf: Configuration): FileSystem = {
    getHadoopFileSystem(new URI(path))
  }

  def getHadoopFileSystem(uri: URI)(implicit conf: Configuration): FileSystem = {
    FileSystem.get(uri, conf)
  }

  def listFile(hadoopRoot: String, path: File)(implicit conf: Configuration): Array[File] = {
    val fileSystem = getHadoopFileSystem(hadoopRoot)
    FileUtil.listFiles(path)
  }

  def getFileNames(path: String)(implicit conf: Configuration): List[String] = {
    val fileSystem = getHadoopFileSystem(path)
    fileSystem.listStatus(new Path(path)).toList.map(x=>path + "/" + x.getPath().getName()

    )
  }

  //Method to convert the parquet file to single CSV file and move from ADLS to Blob Location
  def copyParquetCSVFiles(sqlContext: SQLContext,srcPath:String,errorPath: String, dstPath:String, preappend:String):Unit = {

    println("Writing CSV File")
    val df=sqlContext.read.parquet(srcPath)
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(errorPath)
    println("Retrieving error path")
    val fileSystem1 = getHadoopFileSystem(errorPath)
    val r = fileSystem1.listStatus(new Path(errorPath))
    var csvFileName = new ArrayBuffer[String]
    val filestocopy = r.filter(p => (!(p.getPath().getName().split("\\.").last.equals("crc"))
      && !(p.getPath().getName().split("\\.").last.equals("_SUCCESS")))).map(x => x.getPath)

    filestocopy.map(f=> csvFileName+=f.getName())
    println(errorPath+"/"+csvFileName(0))

    println("Move CSV")
    val hdfsConfig = new Configuration()
    val srcNewFile = errorPath+"/"+csvFileName(0)
    val fileSystem = getHadoopFileSystem(srcNewFile)
    println(srcNewFile)
    val hdfs = FileSystem.get(new URI(srcNewFile), hdfsConfig)
    val oriPath = new Path(srcNewFile)
    val newTgtFile = dstPath+"/"+preappend+".csv"
    val targetFile = new Path(newTgtFile)
    println(targetFile)

    val srcFileSystem = getHadoopFileSystem(srcPath)
    val destFileSystem = getHadoopFileSystem(newTgtFile)
    FileUtil.copy(fileSystem, oriPath, destFileSystem, targetFile, false, true, conf)
  }
  //This function will return the file name under the directory specified.
  //Not it will only return the only the first file name from the provided directory
  def retrieveFileName(path: String):String= {
    implicit val conf = SparkHadoopUtil.get.newConfiguration(new SparkConf())
    val fileSystem1 = FileSystem.get(new URI(path), conf)
    val r = fileSystem1.listStatus(new Path(path))
    var fileName = new ArrayBuffer[String]
    val filestocopy = r.map(x => x.getPath)
    filestocopy.map(f => fileName += f.getName())
    fileName(0)
  }
  
  
  /** Check if the file exists at the given path. */
  def checkFileExists(path: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()   
    val hdpPath = new Path(path)
    val fs = getFileSystemForPath(hdpPath, hadoopConf)
    fs.isFile(hdpPath)
  }
  
  def getFileSystemForPath(path: Path, conf: Configuration): FileSystem = {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    
    val fs = path.getFileSystem(conf)
    fs match {
      case localFs: LocalFileSystem => localFs.getRawFileSystem
      case _ => fs
    }
  }
  
  def checkFolder(path: String, sc: SparkContext):Boolean={
    FileSystem.get(new URI(path), sc.hadoopConfiguration).exists(new Path(path))
  }
}