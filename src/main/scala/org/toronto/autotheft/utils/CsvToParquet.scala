package org.toronto.autotheft.utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, udf}

object CsvToParquet {
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Toronto AutoTheft Project")
      .getOrCreate()
  }
}
