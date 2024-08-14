package org.toronto.autotheft.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Aggregation extends App {

  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  val spark: SparkSession = SparkSession.builder().master("local[1]")
    .appName("Toronto AutoTheft Project")
    .getOrCreate()


}
