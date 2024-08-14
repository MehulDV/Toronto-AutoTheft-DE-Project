package org.toronto.autotheft.lowlevelapi

//Imports
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDParallelize {

  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Toronto AutoTheft Project")
      .getOrCreate()

    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val rddCollect = rdd.collect()
    println("Number of Partitions: " + rdd.getNumPartitions)
    println("Action: First element" + rdd.first())
    rddCollect.foreach(println)

    rdd.saveAsTextFile("path")

  }
}