package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util._


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro
import org.apache.spark.sql.functions._
object june04task {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()

println("Task 1 04-june-2022")
    val df = spark.read.format("xml").option("rowTag","POSLog").load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\transactions.xml")
    df.show(10)
    println("Task 2 04-june-2022")
    df.printSchema()



  }

}
