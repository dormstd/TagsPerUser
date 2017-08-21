package com.ricardo

import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.regexp_extract
import org.slf4j.LoggerFactory

/**
  * Created by RicardoRuizSaiz on 29/06/2017.
  */

object TagsPerUser {
  def main(args: Array[String]) {

    /**
      *
      * Expected parameters:
      * (0) win -> if run on windows, creates the spark context acordingly (only for testing)
      * (1) local -> tell the spark context to run locally
      * (2)
      *
      */
    val LOG = LoggerFactory.getLogger(getClass)
    //Property for running locally on windows
    if (args.length<2)
    {
      LOG.error("Incorrect number of parameters. Expecting 2, got "+args.length+1)
      sys.exit(1)
    }
    if(args(0).equals("win"))
    {
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    }

    //Create spark session with hive support
    val session = if(args(0).equals("win")) {
      SparkSession.builder()
        .master(args(1))
        .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
        .enableHiveSupport()
        .getOrCreate()
    }else{
      SparkSession.builder()
        .master(args(1))
        //.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
        .enableHiveSupport()
        .getOrCreate()
    }

    //Import implicits from spark session
    //This allows to refer to the columns of the DF by name: $"columnName"
    import session.implicits._

    val customSchema = StructType(Array(
      StructField("Timestamp", TimestampType, true),
      StructField("userID", StringType, true),
      StructField("RESTMethod", StringType, true),
      StructField("RESTURL", StringType, true),
      StructField("AppVersion", StringType, true)))

    //Load the data files
    val inputData = session.read
      // library Doc: https://github.com/databricks/spark-csv
      .format("com.databricks.spark.csv")
      .schema(customSchema)
      .option("delimiter", "\t")
      .csv("C:\\Users\\Ricardo.RuizSaiz\\Desktop\\logs\\2017-08-01-18.tsv")

    val filteredTagsPerUser = inputData
      .filter(!($"userID".rlike("""/d+""")))
      .filter($"RESTURL".contains("""type=tag_details"""))
      .groupBy($"userID").count()

    //Write data to the postgresql table
    //writeToPostgresql(filteredTagsPerUser,"tagsPerUser")

    filteredTagsPerUser.take(10).foreach(println)

  }

  def writeToPostgresql (tableData: DataFrame, tabla : String): Unit =
  {

    //All this needs to be in a properties file but it is in the TO-DO for now
    val url = "jdbc:postgresql://198.123.43.24:5432/kockpit"
    val prop = new Properties()
    prop.setProperty("user","postgres")
    prop.setProperty("password","password")
    prop.setProperty("driver","org.postgresql.Driver")
    prop.setProperty("mode","overwrite")

    tableData.write.jdbc(url, tabla, prop)
  }

}
