import JoinUtils.{DataA, DataB, JoinedDS}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfter
import org.scalatest.funspec.AnyFunSpec

class JoinRunnerTest extends AnyFunSpec with BeforeAndAfter with DatasetSuiteBase {

  import spark.implicits._

  // Define test data
  private val testDataA: Seq[DataA] = Seq(
    DataA("1", "Country1", "IP1", "URL1"),
    DataA("2", "Country2", "IP2", "URL2"),
    DataA("3", "Country3", "IP3", "URL3")
  )
  private val testDataB: Seq[DataB] = Seq(
    DataB("URL1", "PageInfo1"),
    DataB("URL2", "PageInfo2"),
    DataB("URL3", "PageInfo3")
  )

  before {
    // Create test DataFrames
    val dataA: DataFrame = testDataA.toDF()
    val dataB: DataFrame = testDataB.toDF()

    // Register test DataFrames as temporary views
    dataA.createOrReplaceTempView("dataA")
    dataB.createOrReplaceTempView("dataB")
  }

  describe("JoinRunner") {
    it("naiveJoin should return the correct result") {
      val expectedData = Seq(
        JoinedDS("1", "Country1", "IP1", "URL1", "PageInfo1"),
        JoinedDS("2", "Country2", "IP2", "URL2", "PageInfo2"),
        JoinedDS("3", "Country3", "IP3", "URL3", "PageInfo3")
      )

      val expectedDataFrame = spark.createDataFrame(expectedData)

      val result = JoinRunner.naiveJoin(spark.table("dataA").as[DataA], spark.table("dataB").as[DataB]).as[JoinedDS]
      assertDatasetEquals(expectedDataFrame.as[JoinedDS], result)
    }

    it("bucketJoin should return the correct result") {
      val expectedData = Seq(
        JoinedDS("1", "Country1", "IP1", "URL1", "PageInfo1"),
        JoinedDS("2", "Country2", "IP2", "URL2", "PageInfo2"),
        JoinedDS("3", "Country3", "IP3", "URL3", "PageInfo3")
      )

      val expectedDataFrame = spark.createDataFrame(expectedData)

      val result = JoinRunner.bucketJoin(spark.table("dataA").as[DataA], spark.table("dataB").as[DataB])(spark)
      assertDatasetEquals(expectedDataFrame.as[JoinedDS], result.as[JoinedDS])
    }
  }

  after {
    // Clean up temporary views
    spark.catalog.dropTempView("dataA")
    spark.catalog.dropTempView("dataB")
  }

}
