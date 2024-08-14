package org.toronto.autotheft.dataset

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.toronto.autotheft.schema.TheftRecord

object AutoTheftDS {
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  val spark: SparkSession = SparkSession.builder().master("local[1]")
    .appName("Grocery Data Analysis")
    .getOrCreate()

  import  spark.implicits._

  val autoTheftDataDataFrame = spark.read.parquet("dbfs:/FileStore/tables/output/Auto_Theft_Open_Data_1.parquet")
  val autoTheftDataDS : Dataset[TheftRecord] = autoTheftDataDataFrame.as[TheftRecord]

  // What is the total number of auto thefts reported?
   val autoThefTotalCount = autoTheftDataDS.count()

  // How many auto thefts were reported each year
  val yearByTheftDF = autoTheftDataDS
    .filter($"REPORT_YEAR" < 2024)
    .groupBy("REPORT_YEAR")
    .agg(count("*").as("Stolen_Vehicles"))
    .orderBy($"Stolen_Vehicles".desc)

  yearByTheftDF.show()

  // How many auto thefts occurred in each month?

  val monthOrder =
    when(col("REPORT_MONTH") === "January", 1)
      .when(col("REPORT_MONTH") === "February", 2)
      .when(col("REPORT_MONTH") === "March", 3)
      .when(col("REPORT_MONTH") === "April", 4)
      .when(col("REPORT_MONTH") === "May", 5)
      .when(col("REPORT_MONTH") === "June", 6)
      .when(col("REPORT_MONTH") === "July", 7)
      .when(col("REPORT_MONTH") === "August", 8)
      .when(col("REPORT_MONTH") === "September", 9)
      .when(col("REPORT_MONTH") === "October", 10)
      .when(col("REPORT_MONTH") === "November", 11)
      .when(col("REPORT_MONTH") === "December", 12)
      .otherwise(0)

  val monthWiseDF = autoTheftDataDS
    .groupBy("REPORT_YEAR", "REPORT_MONTH")
    .agg(count("*").as("count"))
    .orderBy(col("REPORT_YEAR"), monthOrder)

  monthWiseDF.show()

  // How many auto thefts occurred on each day of the week?
  val totalCountDF = autoTheftDataDS
    .groupBy("OCC_DOW")
    .agg(count("*").as("Total_Thefts"))
    .orderBy("OCC_DOW")
  totalCountDF.show()

  // Hourly trend for vehicle getting stolen

  val hourlyDF = autoTheftDataDS
    .groupBy("OCC_HOUR")
    .agg(count("*").as("Total_Thefts"))
    .orderBy(col("Total_Thefts").desc)
  hourlyDF.show()

  // Which police division has the highest number of auto thefts?
  val divisionDF = autoTheftDataDS
    .groupBy("DIVISION")
    .agg(count("*").as("total_thefts"))
    .orderBy(col("total_thefts").desc)
    .limit(1)
  divisionDF.show()

  // How many auto thefts occurred in each neighbourhood (158 neighbourhoods)?
  val areaDF = autoTheftDataDS
    .groupBy("NEIGHBOURHOOD_158")
    .agg(count("*").as("total_count"))
    .orderBy(col("total_count").desc)
    .limit(10)
  areaDF.show()
  // Most stolen vehicles categorized by NEIGHBOURHOOD_140

  val NEIGHBOURHOOD_140DF = autoTheftDataDS
    .groupBy("NEIGHBOURHOOD_140")
    .agg(count("*").as("total_count"))
    .orderBy(col("total_count").desc)
    .limit(10)
  NEIGHBOURHOOD_140DF.show()

  // How many auto thefts occurred in each division from 2019 to 2023?

  val divisionYearDF = autoTheftDataDS
    .filter(col("OCC_YEAR").between(2019, 2023))
    .groupBy("OCC_YEAR", "DIVISION")
    .agg(count("*").as("total_count"))
    .orderBy("DIVISION", "OCC_YEAR")
  divisionYearDF.show()

  val groupedDF = autoTheftDataDS
    .groupBy("NEIGHBOURHOOD_140", "REPORT_YEAR")
    .agg(count("*").as("Total_Thefts"))
    .orderBy("NEIGHBOURHOOD_140", "REPORT_YEAR")
  groupedDF.show()


  // Division wise stolen count

  val divisionTotalDF = autoTheftDataDS
    .groupBy("DIVISION")
    .agg(count("*").as("total_count"))
    .orderBy(col("total_count").desc)
  divisionTotalDF.show()

  // Least stolen vehicles categorized by NEIGHBOURHOOD_140
  val leastStolenDF = autoTheftDataDS
    .groupBy("NEIGHBOURHOOD_140")
    .agg(count("*").as("total_count"))
    .orderBy(col("total_count").asc)
    .limit(10)

  leastStolenDF.show()

  // Division D23 area wise stolen vehicle count
  val divisionD23DF = autoTheftDataDS
    .filter(col("DIVISION") === "D23")
    .groupBy("NEIGHBOURHOOD_140")
    .agg(count("*").as("total_count"))
    .orderBy(col("total_count").desc)
  divisionD23DF.show()

  // Division D23 area wise Count for Year 2023
  val d23Year23DF = autoTheftDataDS
    .filter(col("REPORT_YEAR") === 2023 && col("DIVISION") === "D23")
    .groupBy("NEIGHBOURHOOD_140")
    .agg(count("*").as("total_count"))
    .orderBy(col("total_count").desc)
  d23Year23DF.show()

  // West Humber-Clairville all year trend
  val westHumberDF = autoTheftDataDS
    .filter(col("NEIGHBOURHOOD_140") === "West Humber-Clairville (1)" && col("REPORT_YEAR") < 2024)
    .groupBy("REPORT_YEAR")
    .agg(count("*").as("TOTAL_COUNT"))
    .orderBy(col("TOTAL_COUNT").desc)
  westHumberDF.show()

  // Year 2023 - Month wise stolen count
  // Define a mapping of months to their numeric values
  val monthOrderMap = Map(
    "January" -> 1,
    "February" -> 2,
    "March" -> 3,
    "April" -> 4,
    "May" -> 5,
    "June" -> 6,
    "July" -> 7,
    "August" -> 8,
    "September" -> 9,
    "October" -> 10,
    "November" -> 11,
    "December" -> 12
  )

  val year2023DF = autoTheftDataDS
    .filter(col("OCC_YEAR") === 2023)
    .groupBy("OCC_MONTH")
    .agg(count("*").as("COUNT_TOTAL"))
    .orderBy(
      expr("CASE " +
        monthOrderMap.map { case (month, num) => s"WHEN OCC_MONTH = '$month' THEN $num" }.mkString(" ") +
        " END")
    )
  year2023DF.show()

  // January month trend from 2014 to 2024

  val janDF = autoTheftDataDS
    .filter(col("REPORT_YEAR").between(2014, 2024) && col("REPORT_MONTH") === "January")
    .groupBy("REPORT_YEAR", "REPORT_MONTH")
    .agg(count("*").as("count"))
    .orderBy("REPORT_YEAR")
  janDF.show()

}
