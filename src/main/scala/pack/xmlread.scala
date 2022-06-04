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


object xmlread {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()


    val xmldf = spark.read.format("xml")
      .option("rowTag", "book")
      .load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\book.xml")
    xmldf.show()


    //xmldf.write.mode("overwrite").partitionBy("genre").format("csv").save("C:\\Users\\ppasula1\\Desktop\\Spring boot\\partitionby")

    println("This is from DSL")
    val df=xmldf.select("author","description")
    df.show()

  }

}
