package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util._


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object task {

  case class schema(id: String, date: String, category: String, product: String)


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._
    val file = sc.textFile("C:\\Users\\ppasula1\\Desktop\\data.txt")
    file.foreach(println)

    val mapsplit = file.map(x => x.split(","))
    val schemardd = mapsplit.map(x => schema(x(0), x(1), x(2), x(3)))
    val filterdata = schemardd.filter(x => x.product.contains("product1"))
    val df = filterdata.toDF()
    df.show()
    df.coalesce(1).write.parquet("C:\\Users\\ppasula1\\Desktop\\parquetdata")
    println("CONVERT DIRECTLY SCHEMA RDD TO DF")
    val df1=schemardd.toDF();
    df1.show()
    println("CONVERT data frame to table")
    df1.createOrReplaceTempView("txndf")
    println("Getting data from table")
    val procdf=spark.sql("select * from txndf where product like '%product1%'")
    procdf.show()
  }
}