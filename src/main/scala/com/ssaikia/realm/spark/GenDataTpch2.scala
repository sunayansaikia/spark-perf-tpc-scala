package com.ssaikia.realm.spark

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpch.TPCHTables
import org.apache.spark.sql.SparkSession

object GenDataTpch2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark Hive Generate TPC-H data").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // Note: Declare "sqlContext" for Spark 2.x version
    val sqlContext = spark.sqlContext

    // Set:
    val databaseName = "tpch3" // name of database to create.
    // Note: Here my env is using MapRFS, so I changed it to "hdfs:///tpch".
    // Note: If you are using HDFS, the format should be like "hdfs://namenode:9000/tpch"
    val rootDir = "s3a://data/spark/tpch/" + databaseName // root directory of location to create data in.


    val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    // Run:
    val tables = new TPCHTables(sqlContext, "/tools/tpch-dbgen", // location of tpch-ket/tools directory where dsdgen executable is in the spark executor nodes
      scaleFactor, false, // true to replace DecimalType with DoubleType
      false) // true to replace DateType with StringType

    val tableFilter = "";
    tables.genData(rootDir, format, true, // overwrite the data that is already there
      false, // create the partitioned fact tables
      false, // shuffle to get partitions coalesced into single files.
      false, // true to filter out the partition with NULL key value
      tableFilter, // "" means generate all tables or use 'customer'
      2) // how many dsdgen partitions to run - number of input tasks.

    // Create the specified database
    //spark.sql("create database if not exists " + databaseName)
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    //tables.createExternalTables(rootDir, format, databaseName, true, false, tableFilter)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    //tables.analyzeTables(databaseName, true, tableFilter)
  }

}
