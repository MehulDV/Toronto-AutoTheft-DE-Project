package org.toronto.autotheft.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameSQL extends App {
  System.setProperty("hadoop.home.dir", "C:\\Hadoop\\")

  val spark: SparkSession = SparkSession.builder().master("local[1]")
    .appName("Grocery Data Analysis")
    .getOrCreate()

  val df = spark.read.format("csv").option("header", "true").option("inferschema", "true")
    .load("dbfs:/FileStore/tables/Auto_Theft_Open_Data.csv")
  df.show()
  df.write.mode("overwrite").parquet("dbfs:/FileStore/tables/output/Auto_Theft_Open_Data.parquet")

  val dfParquet = spark.read.parquet("dbfs:/FileStore/tables/output/Auto_Theft_Open_Data.parquet")
  dfParquet.show()

  dfParquet.createOrReplaceTempView("Auto_Theft_Open_Data")

  // What is the total number of auto thefts reported?
  val total_AutoTheft_Count = spark.sql(""" SELECT count(*) FROM Auto_Theft_Open_Data """)
  total_AutoTheft_Count.show()

  // How many auto thefts were reported each year
  val yearByTheftDF = spark.sql(
    """
SELECT REPORT_YEAR, count(*) as Stolen_Vehicles FROM Auto_Theft_Open_Data
WHERE REPORT_YEAR < 2024
GROUP BY REPORT_YEAR
ORDER BY Stolen_Vehicles DESC
""")
  yearByTheftDF.show()

  // How many auto thefts occurred in each month by Each Year?
  val monthWiseDF = spark.sql(
    """
SELECT REPORT_YEAR, REPORT_MONTH, count(*) FROM Auto_Theft_Open_Data
GROUP BY REPORT_YEAR, REPORT_MONTH
ORDER BY
	CASE
		WHEN REPORT_MONTH = 'January' THEN 1
		WHEN REPORT_MONTH = 'February' THEN 2
		WHEN REPORT_MONTH = 'March' THEN 3
		WHEN REPORT_MONTH = 'April' THEN 4
		WHEN REPORT_MONTH = 'May' THEN 5
		WHEN REPORT_MONTH = 'June' THEN 6
		WHEN REPORT_MONTH = 'July' THEN 7
		WHEN REPORT_MONTH = 'August' THEN 8
		WHEN REPORT_MONTH = 'September' THEN 9
		WHEN REPORT_MONTH = 'October' THEN 10
		WHEN REPORT_MONTH = 'November' THEN 11
		WHEN REPORT_MONTH = 'December' THEN 12
	END
	""")
  monthWiseDF.show()

  // How many auto thefts occurred on each day of the week?
  val total_count = spark.sql(
    """ SELECT OCC_DOW, COUNT(*) AS Total_Thefts
      FROM Auto_Theft_Open_Data
      GROUP BY OCC_DOW
      ORDER BY OCC_DOW """);
  total_count.show()

  //Hourly trend for vehicle getting stolen
  val hourlyDF = spark.sql(
    """
    SELECT OCC_HOUR, COUNT(*) AS Total_Thefts
    FROM Auto_Theft_Open_Data
    GROUP BY OCC_HOUR
    ORDER BY Total_Thefts desc
    """)
  hourlyDF.show()

  // Which police division has the highest number of auto thefts?
  val divisionDF = spark.sql(
    """
  SELECT DIVISION, COUNT (*) as total_thefts
  FROM Auto_Theft_Open_Data
  GROUP by DIVISION
  ORDER by total_thefts desc
  LIMIT 1
""")
  divisionDF.show()

  // How many auto thefts occurred in each neighbourhood (158 neighbourhoods)?

  val areaDF = spark.sql(
    """
      SELECT NEIGHBOURHOOD_158, count(*) as total_count FROM Auto_Theft_Open_Data
      GROUP by NEIGHBOURHOOD_158
      ORDER BY total_count DESC
      LIMIT 10
    """)
  areaDF.show()

  // Most stolen vehicles categorized by NEIGHBOURHOOD_140
  val NEIGHBOURHOOD_140DF = spark.sql(
    """
SELECT NEIGHBOURHOOD_140, count(*) as total_count FROM Auto_Theft_Open_Data
GROUP by NEIGHBOURHOOD_140
ORDER BY total_count DESC
LIMIT 10""")
  NEIGHBOURHOOD_140DF.show()

  // How many auto thefts occurred in each division from 2019 to 2023?
  val divisionYearDF = spark.sql(
    """
SELECT OCC_YEAR, DIVISION,  count(*) as total_count FROM Auto_Theft_Open_Data
WHERE OCC_YEAR BETWEEN 2019 AND 2023
GROUP by OCC_YEAR, DIVISION
ORDER BY  DIVISION, OCC_YEAR
""")
  divisionYearDF.show()

  val groupedDF = spark.sql(
    """
SELECT
    NEIGHBOURHOOD_140, REPORT_YEAR, COUNT(*) AS Total_Thefts
FROM
    Auto_Theft_Open_Data
GROUP BY
    NEIGHBOURHOOD_140, REPORT_YEAR
ORDER BY
    NEIGHBOURHOOD_140, REPORT_YEAR ASC;
""")
  groupedDF.show()

  // Division wise stolen count
  val divisionTotalDF = spark.sql(
    """
    SELECT DIVISION, count(*) as total_count FROM Auto_Theft_Open_Data
    GROUP by DIVISION
    ORDER by total_count DESC;
  """)
  divisionTotalDF.show()

  //Least stolen vehicles categorized by NEIGHBOURHOOD_140

  val leastStolenDF = spark.sql(
    """
    SELECT  NEIGHBOURHOOD_140, count(*) as total_count FROM Auto_Theft_Open_Data
    GROUP by  NEIGHBOURHOOD_140
    ORDER by total_count ASC
    LIMIT 10
""")
  leastStolenDF.show()

  // Division D23 area wise stolen vehicle count
  val divisionD23DF = spark.sql(
    """
    SELECT NEIGHBOURHOOD_140, count(*) as total_count FROM Auto_Theft_Open_Data
    WHERE DIVISION = 'D23'
    GROUP by NEIGHBOURHOOD_140
    ORDER by total_count DESC""")
  divisionD23DF.show()

  // Division D23 area wise Count for Year 2023
  val d23Year23DF = spark.sql(
    """
    SELECT NEIGHBOURHOOD_140, count(*) as total_count FROM Auto_Theft_Open_Data
    WHERE REPORT_YEAR = 2023 AND DIVISION = 'D23'
    GROUP by NEIGHBOURHOOD_140
    ORDER by total_count DESC""")
  d23Year23DF.show()

  // West Humber-Clairville all year trend
  val westHumberDF = spark.sql(
    """
    SELECT REPORT_YEAR, COUNT(*) AS TOTAL_COUNT FROM Auto_Theft_Open_Data
    WHERE NEIGHBOURHOOD_140 = 'West Humber-Clairville (1)' AND REPORT_YEAR < 2024
    GROUP BY REPORT_YEAR
    ORDER BY TOTAL_COUNT DESC
""")
  westHumberDF.show()

  // Year 2023 - Month wise stolen count
  val year2023DF = spark.sql(
    """
    SELECT OCC_MONTH, count(*) AS COUNT_TOTAL FROM Auto_Theft_Open_Data
    WHERE OCC_YEAR = 2023
    GROUP by OCC_MONTH
    ORDER BY
        CASE
            WHEN OCC_MONTH = 'January' THEN 1
            WHEN OCC_MONTH = 'February' THEN 2
            WHEN OCC_MONTH = 'March' THEN 3
            WHEN OCC_MONTH = 'April' THEN 4
            WHEN OCC_MONTH = 'May' THEN 5
            WHEN OCC_MONTH = 'June' THEN 6
            WHEN OCC_MONTH = 'July' THEN 7
            WHEN OCC_MONTH = 'August' THEN 8
            WHEN OCC_MONTH = 'September' THEN 9
            WHEN OCC_MONTH = 'October' THEN 10
            WHEN OCC_MONTH = 'November' THEN 11
            WHEN OCC_MONTH = 'December' THEN 12
        END
  """)
  year2023DF.show()

  // January month trend from 2014 to 2024
  val janDF = spark.sql(
    """
      SELECT REPORT_YEAR, count(*) FROM Auto_Theft_Open_Data
      WHERE REPORT_YEAR BETWEEN 2014 AND 2024
      AND REPORT_MONTH = 'January'
      GROUP by REPORT_YEAR, REPORT_MONTH
      ORDER BY REPORT_YEAR
  """)
  janDF.show()

}
