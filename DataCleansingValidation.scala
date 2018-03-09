package com.ppg.dataValidation

import com.ppg.util.AppConfig
import org.apache.spark.sql.SparkSession
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import com.ppg.util.DataValidationUtil._
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{ udf, lit, col, broadcast }

import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.Column
import scala.collection.mutable.ListBuffer
import com.ppg.util.FileHandler._
import org.apache.spark.sql.types.StringType
import com.ppg.util.HadoopUtil._
import org.apache.spark.sql.types.StructType
import com.ppg.util.LogHandler

object DataCleansingValidation {
  val config = AppConfig.getConfig("DataCleansingValidation")
  implicit val spark = SparkSession.builder().appName("DataCleansing - DataValidation").getOrCreate()
  implicit val sc = spark.sparkContext
  implicit val sqlContext = spark.sqlContext
  import spark.implicits._
  
  spark.conf.set("spark.sql.crossJoin.enabled", "true")
  val YYYYMMDDHHMISS = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
  val processStartDt = DateTime.now().toString(YYYYMMDDHHMISS)
  val today = DateTime.now().toString(YYYYMMDDHHMISS)
  val numPartitions = config.getString("num_of_partition").toInt
  val delimiterChar = config.getString("delimiterChar")
  
  val YYYY = DateTimeFormat.forPattern("YYYY")
  val MM = DateTimeFormat.forPattern("MM")
  val DD = DateTimeFormat.forPattern("dd")
  val yyyyToday = DateTime.now().toString(YYYY)
  val mmToday = DateTime.now().toString(MM)
  val ddToday = DateTime.now().toString(DD)
  val regex = "\\[|\\]";
           
  /**
   * errorFlagFuncUdf: Is the udf to flag the error records (which fails null check and date format) with Y and correct records with N
   *  errorDescFuncUdf: Is the Udf to provide generic error description for the error records
   */
  val errorFlagFuncUdf = udf(errorFlagFunc _)
  val errorDescFuncUdf = udf(errorDescFunc _)
  val lowerCaseUdf = udf(lowerCase _)
  val deleteFlagColUdf = udf(deleteFlagCol _)
  val columnNameList = List("column_name")
  
  sqlContext.udf.register("nullOrBlankBooleanUdf", nullOrBlankBooleanUdf _)
  /**
   * @author asrajagopalan
   * @Parm src_table_name is the Raw Viewable table
   * @Parm previous_src_path is the temp path where we keep the previous file with hash key generated.
   * @Parm loadIndicator is the indicator flag which will be used to indicate if the load needs to be load with CDC or no.
   */
  def main(args: Array[String]):Unit={
     LogHandler.log.info("DataValidation: Logging started for "+args(0))
     try{
         LogHandler.debug("DataValidation-"+args(0)+": The Table which is going to be processed :"+args(0))
         val src_table_name = args(0)
         val previous_src_path = args(1)
         val loadIndicator = args(3)
         LogHandler.debug("DataValidation-"+src_table_name+": Step to read audit file has begun ")
         val ruleFileDF = readSrcRules(src_table_name)
         LogHandler.debug("DataValidation-"+src_table_name+": Read audit file will be converted to collection ")
         val (notNullableColumnList,date_column_map,sourceTdyLocation,jobRunId,batchRunId,targetLocation,pkTdyList) = listCreation(ruleFileDF)
         val srcTdyColumn = ruleFileDF.select(columnNameList.head,columnNameList.tail:_*).map(r=> r.toString().replaceAll(regex, "")).collect().filterNot(_.isEmpty())
         val srcTdyColumnListSeq = srcTdyColumn.toList.toSeq
         LogHandler.debug("DataValidation-"+src_table_name+": The location from where RAW is read is :"+sourceTdyLocation)
         val srcTdyDf = sourceFileLoad(ruleFileDF,sourceTdyLocation,srcTdyColumnListSeq)
                         
        /**
          ******************************************************************************************************************
          * Below if clause checks if there is previous audit sqoop file then CDC is not performed or it is used for first time load
          * Else if clause is used for incremental load without CDC
          * Else it is considered as a fresh new execution and skips the CDC and only performs the validation checks alone
          ****************************************************************************************************************** 
          */
       if((!checkPath(config.getString("rule_prev_src_path")) || previous_src_path.trim.isEmpty()) && loadIndicator == "false"){
         LogHandler.debug("DataValidation-"+src_table_name+": Validation is for the First time as full load")
         LogHandler.debug("DataValidation-"+src_table_name+": Constraint check step is started")
         val (deltaValidatedFirstDF,errorCount) = constraintCheck(srcTdyDf.withColumn("delete_flag", lit("N")),notNullableColumnList,date_column_map,jobRunId,batchRunId,targetLocation,srcTdyColumn.toList,src_table_name)
         LogHandler.debug("DataValidation-"+src_table_name+": Post Data Validation the dataframe is saved to :"+targetLocation+" location")
         LogHandler.debug("DataValidation-"+src_table_name+": Final raw viewable records are moved to ADLS")
         deltaValidatedFirstDF.repartition(numPartitions).write
          .format("com.databricks.spark.csv").mode("overwrite")
          .option("delimiter", delimiterChar)
          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
          .partitionBy("snapshot_year","snapshot_month","snapshot_date")
          .save(targetLocation)
         }
       else if((checkPath(config.getString("rule_prev_src_path")) || !previous_src_path.trim.isEmpty()) && loadIndicator == "false"){
         LogHandler.debug("DataValidation-"+src_table_name+": Validation is for the First time as full load")
         LogHandler.debug("DataValidation-"+src_table_name+": Constraint check step is started")
         val (deltaValidatedFirstDF,errorCount) = constraintCheck(srcTdyDf.withColumn("delete_flag", lit("N")),notNullableColumnList,date_column_map,jobRunId,batchRunId,targetLocation,srcTdyColumn.toList,src_table_name)
         LogHandler.debug("DataValidation-"+src_table_name+": Post Data Validation the dataframe is saved to :"+targetLocation+" location")
         LogHandler.debug("DataValidation-"+src_table_name+": Final raw viewable records are moved to ADLS")
         deltaValidatedFirstDF.repartition(numPartitions).write
          .format("com.databricks.spark.csv").mode("append")
          .option("delimiter", delimiterChar)
          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
          .partitionBy("snapshot_year","snapshot_month","snapshot_date")
          .save(targetLocation)
       }
     else{
       LogHandler.debug("DataValidation-"+src_table_name+": Validation is with Change Data Capture")
       LogHandler.debug("DataValidation-"+src_table_name+": The audit file of previous load is stored to dataframe")
       val rulePrevFileDF = readSrcPrevRules(src_table_name)
       LogHandler.debug("DataValidation-"+src_table_name+": The Previous source location and list of primary key of the previous file is collected")
       
       val (sourcePrevLocation,pkPrevList) = listCreationPrev(rulePrevFileDF)
       val srcPrevColumn = rulePrevFileDF.select(columnNameList.head,columnNameList.tail:_*).map(r=> r.toString().replaceAll(regex, "")).collect().filterNot(_.isEmpty())
       val srcPrevColumnListSeq =(List("hash_key_pk","hash_key_col")++srcPrevColumn.toList).toSeq
       LogHandler.debug("DataValidation-"+src_table_name+": The location from where RAW (previous) is read is :"+previous_src_path)
       val prevTdyDf = sourceFileLoad(rulePrevFileDF,previous_src_path,srcPrevColumnListSeq)
       
       /**
          ******************************************************************************************************************
          * Below step is to identify if any new column is present in the source or any column got deleted.
          * extraTdyColumn: Identifies if any new column new source file
          * extraPrevColumn: Identifies if any column got deleted from the new source file
          ******************************************************************************************************************
          */
         LogHandler.debug("DataValidation-"+src_table_name+": Step to identify the extra column in the recent source or deleted column in the recent file is identified")
         val extraTdyColumn = ((List("hash_key_pk","hash_key_col")++srcTdyDf.columns.toList) filterNot prevTdyDf.columns.toList.contains)
         val extraPrevColumn = (prevTdyDf.columns.toList filterNot (List("hash_key_pk","hash_key_col")++srcTdyDf.columns.toList).contains)
         /**
          ******************************************************************************************************************
          * Below if clause identifies if becomes active if the new source file contains new column. 
          * Then the previous source file data frame is added with those column with null and perform the CDC and contraint checks
          * prevTdyChngDf: is the previous file dataframe which got added with missing or extra column that has come in recent file.
          * columnNameOrder: This list is introduced for dynamic schema. In case column gets added in middle the schema must be made coherant.
          * deltaDF: Is the insert/ update / deleted records post CDC logic.
          * deltaValidatedDF: Is the final dataframe that is delta records post data validation with appened audit columns
          ******************************************************************************************************************
          */
         
         if(extraTdyColumn.length !=0){
           LogHandler.debug("DataValidation-"+src_table_name+": Column added to the previous file for performing the Change Data Capture")
           val prevTdyChngDf = extraTdyColumn.foldLeft(prevTdyDf)((df,name) => df.withColumn(name, lit("")))
           val columnNameOrder = prevTdyChngDf.columns.toList
           LogHandler.debug("DataValidation-"+src_table_name+": Change data Capture Step has started")
           val deltaDF = compareRawFiles(srcTdyDf,prevTdyChngDf,pkTdyList,pkPrevList,columnNameOrder,previous_src_path)
           LogHandler.debug("DataValidation-"+src_table_name+": Constraint check step is started")
           val (deltaValidatedDF,errorCount) = constraintCheck(deltaDF,notNullableColumnList,date_column_map,jobRunId,batchRunId,targetLocation,deltaDF.columns.toList,sourceTdyLocation)
           LogHandler.debug("DataValidation-"+src_table_name+": Post Data Validation the dataframe is saved to :"+targetLocation+" location")
           LogHandler.debug("DataValidation-"+src_table_name+": Final raw viewable records are moved to ADLS")
           deltaValidatedDF.repartition(numPartitions).write
                          .format("com.databricks.spark.csv").mode("append")
                          .option("delimiter", delimiterChar)
                          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
                          .partitionBy("snapshot_year","snapshot_month","snapshot_date")
                          .save(targetLocation)
         }
       /**
          ******************************************************************************************************************
          * Below if clause identifies if becomes active if the new source file has delete columns. 
          * Then the new source file data frame is added with those column with null and perform the CDC and contraint checks
          * srcTdyChngDf: Is a dataframe which got added with missing or extra column that was present in previous file.
          * columnNameOrder: This list is introduced for dynamic schema. In case column gets added in middle the schema must be made coherant.
          * deltaDF: Is the insert/ update / deleted records post CDC logic.
          * deltaValidatedDF: Is the final dataframe that is delta records post data validation with appened audit columns
          ******************************************************************************************************************
          */
         else if(extraPrevColumn.length != 0){
             LogHandler.debug("DataValidation-"+src_table_name+": Column added to the recent file for performing the Change Data Capture")
             val srcTdyChngDf = extraPrevColumn.foldLeft(srcTdyDf)((df,name) => df.withColumn(name, lit("")))
             val columnNameOrder = prevTdyDf.columns.toList
             LogHandler.debug("DataValidation-"+src_table_name+": Change data Capture Step has started")
             val deltaDF = compareRawFiles(srcTdyChngDf,prevTdyDf,pkTdyList,pkPrevList,columnNameOrder,previous_src_path)
             LogHandler.debug("DataValidation-"+src_table_name+": Constraint check step is started")
             val (deltaValidatedDF,errorCount) = constraintCheck(deltaDF,notNullableColumnList,date_column_map,jobRunId,batchRunId,targetLocation,deltaDF.columns.toList,sourceTdyLocation)
             LogHandler.debug("DataValidation-"+src_table_name+": Post Data Validation the dataframe is saved to :"+targetLocation+" location")
             LogHandler.debug("DataValidation-"+src_table_name+": Final raw viewable records are moved to ADLS")
             deltaValidatedDF.repartition(numPartitions).write
                          .format("com.databricks.spark.csv").mode("append")
                          .option("delimiter", delimiterChar)
                          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
                          .partitionBy("snapshot_year","snapshot_month","snapshot_date")
                          .save(targetLocation)
         }
         
         /**
          ******************************************************************************************************************
          * Below case get success when the new source and previous source columns are equally matched
          ******************************************************************************************************************
          */
         else{
             LogHandler.debug("DataValidation-"+src_table_name+": No Change to structure")
             LogHandler.debug("DataValidation-"+src_table_name+": Change data Capture Step has started")
             val deltaDF =compareRawFiles(srcTdyDf,prevTdyDf,pkTdyList,pkPrevList,prevTdyDf.columns.toList,previous_src_path)
             LogHandler.debug("DataValidation-"+src_table_name+": Constraint check step is started")
             val (deltaValidatedDF,errorCount) = constraintCheck(deltaDF,notNullableColumnList,date_column_map,jobRunId,batchRunId,targetLocation,deltaDF.columns.toList,sourceTdyLocation)
             LogHandler.debug("DataValidation-"+src_table_name+": Post Data Validation the dataframe is saved to :"+targetLocation+" location")
             LogHandler.debug("DataValidation-"+src_table_name+": Final raw viewable records are moved to ADLS")
             deltaValidatedDF.repartition(numPartitions).write
                          .format("com.databricks.spark.csv").mode("append")
                          .option("delimiter", delimiterChar)
                          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
                          .partitionBy("snapshot_year","snapshot_month","snapshot_date")
                          .save(targetLocation)
           }
       }
     }
     catch{
       case e: Exception=> println("The Exception is due to :"+e.getMessage) 
       LogHandler.error("DataValidation-"+args(0)+": Job Failed due to:",e)
       System.exit(1)
     }
   }
   
   /**
   *******************************************************************************************************************
   * Below function creates data frame of the audit table
   *******************************************************************************************************************
   */
  def readSrcRules(src_table_name : String):DataFrame={
    val ruleRdd = sc.textFile(config.getString("rule_src_path"), 200)
    val ruleDF = ruleRdd.map{y=> val csvParser = new CSVParser(',')
                  csvParser.parseLine(y)
                  }.map(x=> 
                    src_rule_column(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10))
                  ).toDF()
    ruleDF.as('inp).filter(lowerCaseUdf($"inp.tgt_table_name") === src_table_name.toLowerCase()) 
  }
  
  /**
   *******************************************************************************************************************
   * Below function creates data frame of the audit table
   *******************************************************************************************************************
   */
  def readSrcPrevRules(src_table_name : String):DataFrame={
    val ruleRdd = sc.textFile(config.getString("rule_prev_src_path"), 200)
    val rulePrevDF = ruleRdd.map{y=> val csvParser = new CSVParser(',')
                  csvParser.parseLine(y)
                  }.map(x=> 
                    prev_src_rule_column(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10))
                  ).toDF()
    rulePrevDF.as('inp).filter(lowerCaseUdf($"inp.tgt_table_name") === src_table_name.toLowerCase()) 
  }
  
  
  /**
   *******************************************************************************************************************
   * Below function creates primary key list, date column map of specific table for the data validation
   *******************************************************************************************************************
   */
  def listCreation(ruleDF: DataFrame):(List[String],scala.collection.mutable.Map[String,String],String,String,String,String,List[String])={
    val regex = "\\[|\\]";
    val notNullableColumnList = ruleDF.as('inp).filter($"inp.is_nullable" === "N").select($"inp.column_name").rdd.map(r => r(0).toString()).collect().toList
    val date_column_map = ruleDF.as('inp).filter($"inp.datefieldflag" === "Y").select($"inp.column_name",$"inp.dataformat").rdd.map(r => r(0).toString -> r(1).toString).collectAsMap()
    val sourceTdyLocation = ruleDF.as('inp).filter($"inp.src_path".isNotNull).select($"inp.src_path").distinct.rdd.map(r => r.toString).collect().mkString("").replaceAll(regex, "")
    val targetLocation = ruleDF.as('inp).filter($"inp.tgt_path".isNotNull).select($"inp.tgt_path").distinct.rdd.map(r => r.toString).collect().mkString("").replaceAll(regex, "")
    val jobRunId = ruleDF.as('inp).select($"inp.job_run_id").distinct.rdd.map(r => r.toString).collect().mkString("").replaceAll(regex, "")
    val batchRunId = ruleDF.as('inp).select($"inp.batch_run_id").distinct.rdd.map(r => r.toString).collect().mkString("").replaceAll(regex, "")
    val pkTdyList = ruleDF.as('inp).filter($"inp.primary_key_flag" === "Y").select($"inp.column_name").rdd.map(r => r(0).toString()).collect().toList
    (notNullableColumnList,collection.mutable.Map(date_column_map.toSeq: _*),sourceTdyLocation,jobRunId,batchRunId,targetLocation,pkTdyList)
  }
  
  
  /**
   *******************************************************************************************************************
   * Below function creates primary key list, date column map of specific table for the data validation
   *******************************************************************************************************************
   */
  def listCreationPrev(rulePrevFileDF: DataFrame):(String,List[String])={
    val regex = "\\[|\\]";
    val sourcePrevLocation = rulePrevFileDF.as('inp).filter($"inp.src_path".isNotNull).select($"inp.src_path").distinct.rdd.map(r => r.toString).collect().mkString("").replaceAll(regex, "")
    val pkPrevList = rulePrevFileDF.as('inp).filter($"inp.primary_key_flag" === "Y").select($"inp.column_name").rdd.map(r => r(0).toString()).collect().toList
    (sourcePrevLocation,pkPrevList)
  }
  
   /**
   *******************************************************************************************************************
   * Below function creates dynamic data frame from source file it read from
   *******************************************************************************************************************
   */
  def sourceFileLoad(ruleFileDF: DataFrame,sourceLocation: String,columnNameListSeq: Seq[String]):(DataFrame)={
    
    val actualSrcFileDF = spark.read.format("com.databricks.spark.csv")
                          .option("delimiter", delimiterChar).csv(sourceLocation)
                          .toDF(columnNameListSeq: _*)
    val datacleansedDF = actualSrcFileDF.columns
    .foldLeft(actualSrcFileDF) { (memoDF, colName) =>
      memoDF.withColumn(
        colName,
        emptyToNull(col(colName))
      )
  }
    
    datacleansedDF
  }
  
  /**
   *******************************************************************************************************************
   * Below function checks for length and null records
   *******************************************************************************************************************
   */
  def emptyToNull(c: Column) = when(length(trim(c)) > 0, f(c))
  def f(c: Column):Column={
    if(c.cast(StringType) == null) col(null)
    else if(trim(c).cast(StringType) == "null") col(null)
    else trim(c)
  }
  
  /**
   *******************************************************************************************************************
   * Below function will compare recent and previous records
   *******************************************************************************************************************
   */
  def compareRawFiles(srcFileToday: DataFrame,hashSrcYest: DataFrame,columnPrmkyToday: List[String],columnPrmkyPrev: List[String],columnNameOrder: List[String],tempLocation: String):DataFrame={
    val columnStrTdy = srcFileToday.columns.toList.map(x=> f"""nvl($x,"")""").mkString(",")
    val columnPkToday = columnPrmkyToday.map(x=> f"""nvl($x,"")""").mkString(",")
    
    val hashSrcToday = srcFileToday
                      .withColumn("hash_key_col",expr(f"md5(concat($columnStrTdy))"))
                      .withColumn("hash_key_pk",expr(f"md5(concat($columnPkToday))"))
                      .select(columnNameOrder.head,columnNameOrder.tail:_*)

                          
    if(columnPrmkyPrev.length == 0 || columnPrmkyToday.length == 0){
      val insertUpdateDF = hashSrcToday.join(hashSrcYest,hashSrcToday("hash_key_col") === hashSrcYest("hash_key_col"),"left_outer")
        .filter(hashSrcYest("hash_key_col").isNull)
        .select(hashSrcToday("*"))
        
      insertUpdateDF.withColumn("delete_flag", lit("N")).repartition(numPartitions).write
          .format("com.databricks.spark.csv").mode("overwrite")
          .option("delimiter", delimiterChar)
          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
          .partitionBy("snapshot_year","snapshot_month","snapshot_date")
          .save(tempLocation)
          
      insertUpdateDF.withColumn("delete_flag", lit("N")).drop($"hash_key_pk").drop($"hash_key_col")
    }
    else{
        val deleteRecordDF = hashSrcYest.join(hashSrcToday,hashSrcToday("hash_key_pk") === hashSrcYest("hash_key_pk"),"left_outer")
        .filter(hashSrcToday("hash_key_pk").isNull)
        .select(hashSrcYest("*"))
        
        val rowDF = hashSrcYest.except(deleteRecordDF)
      
        val insertUpdateDF = hashSrcToday.join(rowDF,hashSrcToday("hash_key_col") === rowDF("hash_key_col"),"left_outer")
        .filter(rowDF("hash_key_col").isNull)
        .select(hashSrcToday("*"))
      
        deleteRecordDF.withColumn("delete_flag", lit("Y")).union(insertUpdateDF.withColumn("delete_flag", lit("N"))).repartition(numPartitions).write
          .format("com.databricks.spark.csv").mode("overwrite")
          .option("delimiter", delimiterChar)
          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
          .partitionBy("snapshot_year","snapshot_month","snapshot_date")
          .save(tempLocation)
        
        deleteRecordDF.withColumn("delete_flag", lit("Y")).union(insertUpdateDF.withColumn("delete_flag", lit("N"))).drop($"hash_key_pk").drop($"hash_key_col")
    }
    
  }
  
  
  /**
   *******************************************************************************************************************
   * Below function validates the null check and date format check and provide the final data validated output which is stored in orc format
   *******************************************************************************************************************
   */
  def constraintCheck(actualSrcFileDF: DataFrame,notNullableColumnList: List[String],date_column_map: scala.collection.mutable.Map[String,String],jobRunId: String,batchRunId: String,targetLocation: String,column_names_list: List[String],src_table_name: String):(DataFrame,Long)={
    val finalColumnList = List("error_flag","job_run_id","batch_run_id","delete_flag","snapshot_year","snapshot_month","snapshot_date")++column_names_list
    
    val validatedNotNull = nullConstraint(actualSrcFileDF: DataFrame,notNullableColumnList: List[String])
    //validatedNotNull.repartition(numPartitions).saveDFAsORC(targetLocation+"_exception",config)
    val validateDate = dateConstraint(validatedNotNull,date_column_map)
    
    val finalFullRV = validateDate.as('inp).select($"inp.*")
                      .withColumn("error_flag", errorFlagFuncUdf($"inp.Error_Flag_PK",$"inp.Error_Flag_Date"))
                      .withColumn("error_descrption", errorDescFuncUdf($"inp.Error_Flag_PK",$"inp.Error_Flag_Date"))
                      .drop($"inp.Error_Flag_PK").drop($"inp.Error_Flag_Date")
    
    
    val finalRV = finalFullRV.drop($"error_descrption")
                  .withColumn("job_run_id", lit(jobRunId))
                  .withColumn("batch_run_id", lit(batchRunId))
                  .withColumn("delete_flag", deleteFlagColUdf(finalFullRV("delete_flag")))
                  .withColumn("snapshot_year",lit(yyyyToday))
                  .withColumn("snapshot_month",lit(mmToday))
                  .withColumn("snapshot_date",lit(ddToday))
                  .select(finalColumnList.head,finalColumnList.tail:_*)

                   
     val errorRV = finalFullRV.filter($"error_flag" === "Y")
     LogHandler.debug("DataValidation-"+src_table_name+": Exception records are moved to ADLS")
     errorRV.drop($"error_flag").repartition(numPartitions).write
          .format("com.databricks.spark.csv").mode("overwrite")
          .option("delimiter", delimiterChar)
          .option("codec", "org.apache.hadoop.io.compress.BZip2Codec")
          .save(targetLocation+"_exception")
     (finalRV,errorRV.count)
  }
  
  
  /**
   *******************************************************************************************************************
   * Below function performs null check data validation if there is not nullable column present else it will not perform null check validation
   *******************************************************************************************************************
   */
  
  def nullConstraint(actualSrcFileDF: DataFrame,notNullableColumnList: List[String]):DataFrame={
   if(notNullableColumnList.length !=0){
     validateNotNull(sqlContext,actualSrcFileDF,notNullableColumnList)
   }
   else{
     actualSrcFileDF.withColumn("Error_Flag_PK", lit(""))
   }
  }
  
  /**
   *******************************************************************************************************************
   * Below function performs date check data validation if there is date column present else it will not perform date check validation
   ******************************************************************************************************************* 
   */
  def dateConstraint(validatedNotNull: DataFrame,date_column_map: scala.collection.mutable.Map[String,String]):DataFrame={
   if(date_column_map.size !=0){
     validateDateFormat(sc,sqlContext,validatedNotNull,date_column_map)
   }
   else{
     validatedNotNull.withColumn("Error_Flag_Date",lit(""))
   }
  }
  
  /**
   ******************************************************************************************************************* 
   * Below is the case class for the audit sqoop file
   ******************************************************************************************************************* 
   */
  case class src_rule_column(job_run_id: String,batch_run_id: String,src_table_name: String,	column_name: String,	tgt_table_name: String,	src_path: String,	tgt_path: String,	is_nullable: String,	dataformat: String,	datefieldflag: String,primary_key_flag: String)
  
  
  /**
   ******************************************************************************************************************* 
   * Below is the case class for the audit sqoop file
   ******************************************************************************************************************* 
   */
  case class prev_src_rule_column(job_run_id:String,batch_run_id: String,src_table_name: String,	column_name: String,	tgt_table_name: String,	src_path: String,	tgt_path: String,	is_nullable: String,	dataformat: String,	datefieldflag: String,primary_key_flag: String)
}