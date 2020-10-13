package scalasamples

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset


object Scaffolding {
  private val RIDES_PATH: String = s"file://${System.getProperty("user.dir")}/duocar/raw/rides"
//  private val RIDERS_PATH: String = s"file://${System.getProperty("user.dir")}/duocar/raw/riders"

  def main(args:Array[String]): Unit = {
    println(">> Creating session")
    val spark: SparkSession = SparkSession.builder
                                          .appName("SimpleApp")
                                          .config("spark.master", "local")
                                          // Scala may complain about memory (Java does not...) The following 2 will fix that.
                                          .config("spark.driver.memory",  "471859200")
                                          .config("spark.testing.memory", "2147480000")
                                          .getOrCreate

    val rides: Dataset[Row] = spark.read
                                   .option("delimiter", ",")
                                   .option("inferSchema", true)
                                   .option("header", true)
                                   .csv(RIDES_PATH)
    println(s"Count: ${rides.count}")
    rides.printSchema()

    spark.close()
  }
}
