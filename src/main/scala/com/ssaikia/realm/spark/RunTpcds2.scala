package com.ssaikia.realm.spark

import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

object RunTpcds2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Spark Hive Benchmark TPCDS data").enableHiveSupport().getOrCreate()

    // Note: Declare "sqlContext" for Spark 2.x version
    val sqlContext = spark.sqlContext

    val databaseName = "tpcds3" // name of database with TPCDS data.

    val tableNames = Array("call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "income_band", "inventory", "item", "promotion",
      "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
      "web_sales", "web_site")
    val rootDir = "s3a://data/spark/tpcds/" + databaseName
    //val rootDir = "file:///var/tmp/tpcds/gendata"
    for (i <- 0 to tableNames.length - 1) {
      //sqlContext.read.csv(rootDir + "/" + tableNames{i} + ".csv").createOrReplaceTempView(tableNames{i})
      sqlContext.read.parquet(rootDir + "/" + tableNames{i}).createOrReplaceTempView(tableNames{i})
    }

    val tpcds = new TPCDS(sqlContext)
    // Set:
    val resultLocation = "s3a://data/spark/results/" + databaseName + "_results" // place to write results
    val iterations = 1 // how many iterations of queries to run.

    //===== Customized a query
    //val queries = new CustomizedQuery().q1
    val query_filter = Seq("q1-v2.4", "q2-v2.4") // run subset of queries
    val queries = tpcds.tpcds2_4Queries.filter(q => query_filter.contains(q.name)) // queries to run.
    //=============

    val timeout = 24 * 60 * 60 // timeout, in seconds.
    // Run:

    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)

    experiment.getCurrentResults // or: spark.read.json(resultLocation).filter("timestamp = 1429132621024")
      .withColumn("Name", substring(col("name"), 2, 100))
      .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
      .select("Name", "Runtime").show(false)
  }

}
