import JoinUtils.{DataA, DataB}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

object DataGenerator extends Props {

  def main(args: Array[String]) {
    implicit val spark: SparkSession = SparkSession.builder.appName("Sovrn Assignment").getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext

    val distributionMap = Map("US" -> 0.9, "CA" -> 0.05, "MX" -> 0.05)
    val N = args(0).toInt //final size of the data

    val countries = generateListFromDistribution(distributionMap, N)
    val ids = sc.parallelize((0 until N).map(number => "%06d".format(number)))

    val dataA = ids.zip(countries).map { case (id, country) =>
      DataA(id = id, country = country, url = s"/url/$country/$id")
    }
    val dfA = spark.createDataFrame(dataA)

    val dataB = ids.zip(countries).map { case (id, country) => DataB(url = s"/url/$country/$id") }
    val dfB = spark.createDataFrame(dataB).sample(0.4)

    dfA.write.mode("Overwrite").parquet(dataAPath)
    dfB.write.mode("Overwrite").parquet(dataBPath)
  }


  def generateListFromDistribution(distributionMap: Map[String, Double], N: Int)(implicit sc: SparkContext): RDD[String] = {
    require(N >= 0, "N must be a non-negative value")

    val totalWeight = distributionMap.values.sum
    val normalizedDistributionMap = distributionMap.mapValues(_ / totalWeight)
    val items = normalizedDistributionMap.keys.toList
    val distributionValues = normalizedDistributionMap.values.toList

    val random = new Random()
    val range = sc.parallelize(1 to N)
    range.map { _ =>
      val randValue = random.nextDouble()
      val cumulativeDistribution = distributionValues.scanLeft(0.0)(_ + _).tail
      val selectedItem = items(cumulativeDistribution.indexWhere(_ >= randValue))
      selectedItem
    }
  }
}
