package gcp_scala_pkg
import org.apache.spark.sql.SparkSession
object gcp_scala_code {
  def main(args: Array[String]): Unit =
    {
      val spark = SparkSession.builder().master("local").appName("DataFrame").getOrCreate()
      val df = spark.read.format("csv").option("header", "true").load("gs://birendrabucket-1/folder1/test.csv")
      df.show()
      df.select("Nmae", "Age").show()
    }
}