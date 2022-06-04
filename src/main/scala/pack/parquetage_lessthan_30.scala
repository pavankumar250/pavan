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

object parquetage_lessthan_30 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()


    println("This is Parquet data")
    val parqdf=spark.read.format("parquet").load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\data.parquet")
    parqdf.show()

    println("This is Raw CSV data")
    val csvdf=spark.read.format("csv").option("header","true").load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\usdata.csv")
csvdf.show()
    println("This is Age less than 30")
    csvdf.createOrReplaceTempView("usdata")
    val lessthan30=spark.sql("select * from usdata where age<30")
    lessthan30.show()

    lessthan30.write.mode("append").parquet("C:\\Users\\ppasula1\\Desktop\\Spring boot\\parqdata")

    println("This is ignore mode greaterthan50 ")

    val greaterthan50=spark.sql("select * from usdata where age>30")
    greaterthan50.show()

    greaterthan50.write.mode("ignore").parquet("C:\\Users\\ppasula1\\Desktop\\Spring boot\\parqdata")

    println("This is overwrite mode greaterthan15 ")

    val greaterthan15=spark.sql("select * from usdata where age>15")
    greaterthan15.show()

    greaterthan15.write.mode("overwrite").parquet("C:\\Users\\ppasula1\\Desktop\\Spring boot\\parqdata")



  }

  }
