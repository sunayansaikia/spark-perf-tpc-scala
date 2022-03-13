package com.ssaikia.realm.spark

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession

object GenDataTpcds {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark Hive Generate TPCDS data").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    // Note: Declare "sqlContext" for Spark 2.x version
    val sqlContext = spark.sqlContext

    // Set:
    val databaseName = "tpcds3" // name of database to create.
    // Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpcds".
    // Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpcds"
    val rootDir = "s3a://data/spark/tpcds/" + databaseName // root directory of location to create data in.


    val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    // Run:
    val tables = new TPCDSTables(sqlContext, "/tools/tpcds-kit/tools", // location of tpcds-ket/tools directory where dsdgen executable is in the spark executor nodes
      scaleFactor, false, // true to replace DecimalType with DoubleType
      false) // true to replace DateType with StringType

    val tableFilter = "";
    tables.genData(rootDir, format, true, // overwrite the data that is already there
      true, // create the partitioned fact tables
      true, // shuffle to get partitions coalesced into single files.
      false, // true to filter out the partition with NULL key value
      tableFilter, // "" means generate all tables or use 'customer'
      2) // how many dsdgen partitions to run - number of input tasks.

    // Create the specified database
    spark.sql("create database if not exists " + databaseName)
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(rootDir, "parquet", databaseName, true, true, tableFilter)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    //tables.analyzeTables(databaseName, true, tableFilter)
  }

}
