package com.ppg.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.DateTime
import org.apache.spark.sql.{ DataFrame }
import java.util.HashMap
import org.apache.spark.sql.functions.{ udf, lit, col, broadcast }
import scala.collection.mutable.Map
import org.joda.time.format.DateTimeFormatter
import scala.reflect.runtime.universe
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType


/**
  * Created by 491620 on 1/30/2018.
  */

object DataValidationUtil {
  val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  
  /**
   * validateNotNull function checks whether the primary key column has
   *  Null values and if any, loads them to the error table
   *
   * @param SQLContext
   * @param DataFrame which contains the input table data
   * @param List which contains the primary key columns read from the yaml file
   * @return DataFrame of valid and invalid records
   */

  def validateNotNull(sqlContext: SQLContext, df: DataFrame, primary_key_col_list: List[String]): (DataFrame) = {

    var primaryCheckCorrect = ""
    var primary_check_incorrect = ""

    for (z <- primary_key_col_list) {
      primaryCheckCorrect = primaryCheckCorrect + "and length(trim(" + z + "))>0 and trim("+ z +") !="+f""" "null" """
      primary_check_incorrect = primary_check_incorrect + "OR " + z + " is null OR length(trim(" + z + "))=0 OR trim("+ z +") =="+f""" "null" """
    }
    df.registerTempTable("incoming_data")
    val valid_records_query = "select * from incoming_data where " + (primaryCheckCorrect.drop(3))
    val invalid_records_query = "select * from incoming_data where " + (primary_check_incorrect.drop(2))

    val valid_records = sqlContext.sql(valid_records_query)
    val invalid_records = sqlContext.sql(invalid_records_query)
    valid_records.as('inp).withColumn("Error_Flag_PK", lit("Unique Key Constraint Satisfied"))
    .union(invalid_records.as('inp).withColumn("Error_Flag_PK", lit("Unique Key Constraint Not Satisfied")))
}
  
  /**
   *  ValidateDateFormat function checks if the date fields
   *  in the data frame are in the format expected in yaml file
   *  and if not, load those records to the error table
   *
   * @param SparkContext
   * @param SQLContext
   * @param DataFrame
   *        contains the records which passed the referential integrity check
   * @param HashMap
   * @return DataFrame
   *         data set of all and error records
   */

  def validateDateFormat(sc1: SparkContext, sqlContext: SQLContext, df: DataFrame, date_map: Map[String,String]): (DataFrame) = {
    // take a dataframe as input, get the date fields and its corresponding expected formats from YAML file.
    // If date is not in expected format, error it out or else convert to ISO

    val incoming_data = "incoming_data"
    val transformed_data = "transformed_data"
    val col_test = df.columns
    val cn = col_test.length

    var date_sql_null_check: String = ""
    var date_sql_part = ""
    var sql_temp: String = ""
    var date_actual_sql_null_check: String = ""

    df.registerTempTable(incoming_data)
    sqlContext.udf.register("ConvertDateTime", convertDateTime _)
    sqlContext.udf.register("replaceNA", replaceNA _)
    sc1.broadcast(convertDateTime _)
    
    /**
     ******************************************************************************************************************
     * Below step is used to form the conversion, where clause for the date validation
     ******************************************************************************************************************
     */
    
    for (i <- 0 to cn - 1) {
      var flag: String = col_test(i)
      date_map.foreach(x => {
        if (col_test(i).equalsIgnoreCase(x._1.toString())) {
          var edited_col_name = x._1 + "_changed "
          date_sql_part = date_sql_part + ",ConvertDateTime(trim(" + x._1 + """),"""" + x._2 + """") """ + edited_col_name
          flag = "replaceNA(nvl(" + x._1 + """_changed,""" + x._1 + """)) as """ + x._1
          date_sql_null_check = date_sql_null_check + x._1 + "_changed is  null or "
        }
      })
      sql_temp = sql_temp + "," + flag
    }
    
    /**
     * Below are the dynamic query that is generated to form the where clause, complete dataset, exception dataset
     */
    val sql_final = "select " + sql_temp.drop(1) + " from " + transformed_data
    val date_sql_null_check_final = date_sql_null_check.dropRight(4)
    val sql_initial = "select incoming_data.* " + date_sql_part + " from incoming_data"
    val sql_exception = "select " + sql_temp.drop(1) + " from exceptionTbl"
    
    
    val df_full = sqlContext.sql(sql_initial)
    df_full.registerTempTable(transformed_data)
    
    
    val df_total_dataset = sqlContext.sql(sql_final)
		(df_full.filter(date_sql_null_check_final)).createOrReplaceTempView("exceptionTbl")
		val exceptRec =sqlContext.sql(sql_exception).select(col_test.head, col_test.tail: _*)
    val validRec = df_total_dataset.except(exceptRec)
		validRec.withColumn("Error_Flag_Date",lit("Date Format is Proper")).union(exceptRec.withColumn("Error_Flag_Date",lit("Date Format is not proper")))
  }
  
  /**
   *  ConvertDateTime function converts the date format of the
   *  incoming date field to ISO format and catches the
   *  IllegalArgumentException and NullPointerException
   *
   * @param String
   *        contains the input date value
   * @param String
   *        contains the date format
   * @return DataFrame of valid and invalid records
   */

  def convertDateTime(dt: String, fmt: String): String = {
    if(dt == null || dt == "null"){
      return "NA"
    }
    else{
      try {
          val dateFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern(fmt)
          val jodatime: DateTime = dateFormatGeneration.parseDateTime(dt);
          val str: String = ISOFormatGeneration.print(jodatime);
          str
        } catch {
          case xe: IllegalArgumentException => return null
          case xe: NullPointerException     => return null
        }
    }
  }
  
  def replaceNA(inp: String):String={
    if(inp == "NA") null
    else if(inp == "null") null
    else inp
  }
  
  /**
   ******************************************************************************************************************
   *  errorFlagFunc : UDF for populate error_flag
   *  errorDescFunc : UDF for populating error_message
   *  lowerCase : UDF to convert string to lower case
   ******************************************************************************************************************
   */

  def errorFlagFunc(errorStringPK: String, errorStringDate: String):String={
    if(errorStringPK == "Unique Key Constraint Not Satisfied" || errorStringDate == "Date Format is not proper")
      "Y"
    else 
      "N"
  }
  
  def errorDescFunc(errorStringPK: String, errorStringDate: String):String={
    var exceptionMessage : String =""
    if(errorStringPK == "Unique Key Constraint Not Satisfied"){
      exceptionMessage += "Unique Key Constraint Not Satisfied"
    }
    if(errorStringDate == "Date Format is not proper")
      exceptionMessage += "~Date Column is not in proper format"
    exceptionMessage.stripPrefix("~")
  }
  
  def lowerCase(inpStr: String):String={
    if(inpStr == null){
      ""
    }
    else
      inpStr.trim.toLowerCase()
    }
  
  val csRdrExceptionSchema=
    StructType(
      StructField("header_msg_type", StringType, true) ::
      Nil
      )
      
  def nullOrBlankBooleanUdf(inpStr: String):Boolean={
    if(inpStr == null){
      false
    }
    else if(inpStr.trim().length() == 0){
      false
    }
    else{
      true
    }
      
    }
  
  
  /*
   ******************************************************************************************************************
   * Below udf is defined to correctly update the delete flag to the delta record or the full load records
   ******************************************************************************************************************
   */
  def deleteFlagCol(inp: String):String={
    if(inp == null)
      "N"
    else if(inp.trim == "")
      "N"
    else
      inp
  }
}