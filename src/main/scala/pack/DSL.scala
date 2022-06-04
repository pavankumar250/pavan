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


object DSL {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val csvdf = spark.read.format("csv")
      .option("header",true)
      .load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\allcountry.csv")
    csvdf.show()

val df1=csvdf.select("id","name")
    df1.show()

val df2=df1.filter("country='IND'")
    df2.show()

    val df3=csvdf.filter(column("country")==="IND")
    df3.show()
  }

  }
