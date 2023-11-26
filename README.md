# Meteo Analysis Java Code Documentation

This documentation provides an overview and usage guide for the Java code designed for meteorological analysis using Apache Spark. The code processes meteorological data stored in a CSV file and performs various analyses to derive insights.

## Table of Contents
1. [Introduction](#introduction)
2. [Setup](#setup)
3. [Data Loading](#data-loading)
4. [Descriptive Statistics](#descriptive-statistics)
5. [Temperature Analysis](#temperature-analysis)
6. [Top Weather Stations](#top-weather-stations)
7. [Conclusion](#conclusion)

## Introduction<a name="introduction"></a>
The provided Java code utilizes Apache Spark to analyze meteorological data. It reads a CSV file containing weather-related information, processes the data, and performs several analyses such as calculating descriptive statistics and identifying top weather stations based on temperature extremes.

## Setup<a name="setup"></a>

```java
SparkSession ss = SparkSession.builder().appName("Meteo analysis").master("local[*]").getOrCreate();
ss.sparkContext().setLogLevel("OFF");
```
The code starts by creating a SparkSession named "Meteo analysis" and sets the log level to "OFF" to reduce unnecessary log output.

## Data Loading<a name="data-loading"></a>

```java
Dataset<Row> dataset = ss.read()
        .option("multiline", true)
        .option("inferSchema", true)
        .csv("src/main/resources/2023.csv.gz");

Dataset<Row> df = dataset.toDF("ID", "date", "element", "value", "m-flag", "q-flag", "s-flag", "OBS-TIME");
df.createOrReplaceTempView("data");
```
The code loads the meteorological data from a CSV file, inferring the schema and handling multiline entries. It then creates a DataFrame (df) with specific column names and registers it as a temporary SQL table named "data."

```shell
+-----------+--------+-------+-----+------+------+------+--------+
|         ID|    date|element|value|m-flag|q-flag|s-flag|OBS-TIME|
+-----------+--------+-------+-----+------+------+------+--------+
|AE000041196|20230101|   TMAX|  252|  null|  null|     S|    null|
|AEM00041194|20230101|   TMAX|  255|  null|  null|     S|    null|
|AEM00041217|20230101|   TMAX|  248|  null|  null|     S|    null|
|AEM00041218|20230101|   TMAX|  254|  null|  null|     S|    null|
|AGE00147716|20230101|   TMAX|  193|  null|  null|     S|    null|
|AGM00060351|20230101|   TMAX|  201|  null|  null|     S|    null|
|AGM00060355|20230101|   TMAX|  234|  null|  null|     S|    null|
|AGM00060403|20230101|   TMAX|  241|  null|  null|     S|    null|
|AGM00060425|20230101|   TMAX|  222|  null|  null|     S|    null|
|AGM00060461|20230101|   TMAX|  186|  null|  null|     S|    null|
|AGM00060467|20230101|   TMAX|  167|  null|  null|     S|    null|
|AGM00060468|20230101|   TMAX|  182|  null|  null|     S|    null|
|AGM00060507|20230101|   TMAX|  214|  null|  null|     S|    null|
|AGM00060511|20230101|   TMAX|  170|  null|  null|     S|    null|
|AGM00060515|20230101|   TMAX|  147|  null|  null|     S|    null|
|AGM00060522|20230101|   TMAX|  222|  null|  null|     S|    null|
|AGM00060535|20230101|   TMAX|  137|  null|  null|     S|    null|
|AGM00060536|20230101|   TMAX|  185|  null|  null|     S|    null|
|AGM00060549|20230101|   TMAX|  151|  null|  null|     S|    null|
|AGM00060555|20230101|   TMAX|  181|  null|  null|     S|    null|
+-----------+--------+-------+-----+------+------+------+--------+
only showing top 20 rows
```

## Descriptive Statistics<a name="descriptive-statistics"></a>
```java
df.describe().show();
```

```shell
+-------+-----------+--------------------+--------+------------------+-------+------+--------+-----------------+
|summary|         ID|                date| element|             value| m-flag|q-flag|  s-flag|         OBS-TIME|
+-------+-----------+--------------------+--------+------------------+-------+------+--------+-----------------+
|  count|   31565980|            31565980|31565980|          31565980|2531686| 32494|31565980|         15298017|
|   mean|       null|2.0230580582311496E7|    null|159.20984677174604|   null|  null|     7.0|894.0926973084158|
| stddev|       null|  300.80013497905054|    null| 905.0261601165985|   null|  null|     0.0|468.9150098972718|
|    min|AE000041196|            20230101|    ADPT|             -7874|      B|     D|       7|                0|
|    max|ZI000067983|            20231121|    WT11|             53400|      T|     Z|       a|             2400|
+-------+-----------+--------------------+--------+------------------+-------+------+--------+-----------------+
```
This section prints out descriptive statistics for the loaded data, providing insights into various statistical measures.

## Temperature Analysis<a name="temperature-analysis"></a>
```java
System.out.println("****** Average minimum temperature. ******");
df.filter(col("element").like("TMIN")).show();

System.out.println("****** Average maximum temperature. ******");
df.filter(col("element").like("TMAX")).show();

System.out.println("****** The maximum temperature. ******");
df.select(max("value").as("Maximum temp")).show();

System.out.println("****** The minimum temperature. ******");
df.select(min("value").as("Minimum temp")).show();
```
This part of the code analyzes temperature-related data. It calculates and prints the average minimum and maximum temperatures, as well as the overall minimum and maximum temperatures.

## Top Weather Stations<a name="top-weather-stations"></a>
```java
System.out.println("****** Top 5 hottest weather stations. ******");
df.sqlContext()
        .sql("Select ID, max(value) as maxTemp from data where element like 'TMAX' group By ID order by maxTemp DESC limit 5")
        .show();

System.out.println("****** Top 5 coldest weather stations. ******");
df.sqlContext()
        .sql("Select ID, min(value) as minTemp from data where element like 'TMAX' group By ID order by minTemp DESC limit 5")
        .show();
```
In this section, the code identifies and displays the top 5 weather stations based on the maximum and minimum temperatures recorded.

## Conclusion<a name="conclusion"></a>
```java
ss.stop();
```
The code concludes by stopping the SparkSession, releasing associated resources.

This documentation serves as a guide for understanding and using the provided Java code for meteorological analysis. Users can customize and extend the code based on specific requirements and data sources.