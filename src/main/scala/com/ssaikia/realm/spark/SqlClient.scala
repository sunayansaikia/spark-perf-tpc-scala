package com.ssaikia.realm.spark

import org.apache.spark.sql.SparkSession

object SqlClient {

  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      println("Argument expected: <database> <sql query>")
      return
    }
    val spark = SparkSession.builder.appName("Spark SQL Query Client").enableHiveSupport().getOrCreate()
    val databaseSelected = Some(args(0)).value;
    val query = Some(args(1)).value;
    spark.sql("use " + databaseSelected);
    spark.sql(query).show(false)
  }

}
