/* firstPipeline.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object firstPipeline {
  def main(args: Array[String]) {
    val inputFile = "/home/ulul/homeDepot/data/train.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Spark Testing")
    val sc = new SparkContext(conf)
    val inputData = sc.textFile(inputFile, 2).cache()
    val noHeader = inputData.filter(line => !line.contains("product_title"))
    val productIds = noHeader.map(line => line.split(",")).map(line => line(1))
    val uniqueProductIds = productIds.distinct()
    println("Lines: %s, Unique products: %s".format(productIds.count(), uniqueProductIds.count()))
  }
}
