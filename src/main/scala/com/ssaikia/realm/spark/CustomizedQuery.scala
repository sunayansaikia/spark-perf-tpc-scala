package com.ssaikia.realm.spark

import com.databricks.spark.sql.perf.Benchmark

class CustomizedQuery extends Benchmark {
  import com.databricks.spark.sql.perf.ExecutionMode._
  private val sqlText = "select * from customer limit 10"
  val q1 = Seq(Query(name = "my customized query", sqlText = sqlText, description = "check some customer info", executionMode = CollectResults))
}
