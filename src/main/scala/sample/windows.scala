package com.basic
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._


case class Salary(depName: String, empNo: Long, salary: Long)

object windows {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C://hadoop//")

    //    val spark = (SparkSession
    //      .builder()
    //      .appName("SparkPipeline")
    //      .config("spark.master", "local")
    //      .getOrCreate()
    //      )

    val ss = (SparkSession
      .builder()
      .master("local[*]")
      .appName("DataSet Test")
      .config("spark.sql.warehouse.dir", "C:/iquest_files/" )
      .getOrCreate())

    import ss.implicits._

    val empSalary = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)).toDS

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val byDepName = Window.partitionBy("depName")
    empSalary.withColumn("avg", avg("salary") over byDepName).show

  }
}
