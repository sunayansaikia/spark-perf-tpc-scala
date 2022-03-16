package com.ssaikia.realm.spark

import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.tpch.TPCH
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

object RunTpch {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Spark Hive Benchmark TPC-H data").enableHiveSupport().getOrCreate()

    // Note: Declare "sqlContext" for Spark 2.x version
    val sqlContext = spark.sqlContext

    val tpch = new TPCH(sqlContext)
    // Set:
    val databaseName = "tpch3" // name of database with TPCDS data.
    val resultLocation = "s3a://data/spark/results/" + databaseName + "_results" // place to write results
    val iterations = 1 // how many iterations of queries to run.
    spark.sql("use " + databaseName) //important to be planced before tpch.tpch1_4Queries

    //===== Customized a query
    //val queries = new CustomizedQuery().q1
    //val queries = tpch.queries;  // queries to run.
    val queries = (1 to 5).map { q =>
      val queryContent: String = IOUtils.toString(
        getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
      new Query(s"Q$q", spark.sqlContext.sql(queryContent), description = s"TPCH Query $q",
        executionMode = CollectResults)
    }
    //=============

    val timeout = 24 * 60 * 60 // timeout, in seconds.
    // Run:

    val experiment = tpch.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)

    experiment.getCurrentResults // or: spark.read.json(resultLocation).filter("timestamp = 1429132621024")
      .withColumn("Name", substring(col("name"), 2, 100))
      .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
      .select("Name","Runtime").show(false)
  }

}
