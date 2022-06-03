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

import sys.process._

object avrodata {


  def main(args: Array[String]): Unit = {


    val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()


    println("This is Avro data")
    val df2=spark.read.format("avro").load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\data.avro")
    df2.show()

  }
}
