# Toronto Auto Theft DE Project 🚘

This repository contains open data on auto thefts in Toronto, provided by the Toronto Police Service. The dataset includes detailed records of auto theft incidents, including date, location, and other key information. The data is intended to improve public awareness and assist researchers, policymakers, and community members in understanding and preventing auto theft.

![me](https://github.com/MehulDV/Toronto-AutoTheft-Project/blob/main/src/main/scala/org/toronto/autotheft/blob/Auto_Theft.gif)

## Table of Contents

- [Introduction](#introduction)
- [Dataset Details](#dataset-details)
- [Usage](#usage)
- [Code Examples](#code-examples)
- [Installation](#installation)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Introduction

The Auto Theft Open Data is part of Toronto Police Service's initiative to provide accessible data to the public. The dataset covers reported auto theft incidents across Toronto. This data can be used for analysis, visualization, and building models to identify trends and hotspots of auto theft in the city.

## Dataset Details

- **Source**: [Toronto Police Service Open Data Portal](https://data.torontopolice.on.ca/datasets/TorontoPS::auto-theft-open-data/about)
- **Data Format**: CSV, GeoJSON
- **Frequency**: Updated regularly
- **Data Fields**:
  - `Occurrence Date`: Date and time of the theft
  - `Neighbourhood`: Toronto neighborhood where the theft occurred
  - `Latitude` and `Longitude`: Coordinates of the incident
  - Other relevant fields about the incident

## Usage

This dataset can be used for:
- Data analysis and visualization of auto theft trends in Toronto.
- Building predictive models to anticipate areas with high risks of auto theft.
- Community awareness initiatives and public safety campaigns.

## Code Examples
Here are some examples of how to use the Spark code with the Auto Theft Open Data:

### 1. Loading and Saving the Dataset

The following code demonstrates how to load the dataset from a CSV file, display it, and save it as a Parquet file:

```scala
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferschema", "true")
  .load("dbfs:/FileStore/tables/Auto_Theft_Open_Data.csv")
df.show()

df.write.mode("overwrite").parquet("dbfs:/FileStore/tables/output/Auto_Theft_Open_Data.parquet")
```

### 2. How many auto thefts were reported each year?
This query counts the total number of auto thefts reported in the dataset:

```scala
val yearByTheftDF = autoTheftDataDF
.filter($"REPORT_YEAR" < 2024)
.groupBy("REPORT_YEAR")
.agg(count("*").as("Stolen_Vehicles"))
.orderBy($"Stolen_Vehicles".desc)
display(yearByTheftDF)
```

<a href="url"><img src="https://github.com/user-attachments/assets/3246cea0-25e5-41ad-a159-d177d8546f31" height=40% width=40% ></a>


## Installation

To access the data:

1. **Download the dataset** from the [Toronto Police Open Data Portal](https://data.torontopolice.on.ca/datasets/TorontoPS::auto-theft-open-data/about).
2. Load the dataset into your preferred analysis tool (e.g., Python, Scala, Spark, R).

## License

This dataset is made available under the [Open Government License – Toronto](https://open.toronto.ca/open-data-license/).

## Acknowledgements

- Thanks to the [Toronto Police Service](https://www.torontopolice.on.ca/) for providing the Auto Theft Open Data.
- Special recognition to the City of Toronto for supporting open data initiatives and making public safety data accessible to the community.
