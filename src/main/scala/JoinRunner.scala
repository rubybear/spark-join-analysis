import JoinUtils.{DataA, DataB, JoinedDS}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.io.StdIn
import scala.util.Try

object JoinRunner extends Props {

  def main(args: Array[String]) = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Sovrn Assignment").getOrCreate()
    //disable broadcast join for exercise
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    import spark.implicits._

    val dataA: Dataset[DataA] = spark.read.parquet(dataAPath).as[DataA]
    val dataB: Dataset[DataB] = spark.read.parquet(dataBPath).as[DataB]

    val naiveJoinDS: Dataset[JoinedDS] = naiveJoin(dataA, dataB).as[JoinedDS]
    val bucketJoinDS: Dataset[JoinedDS] = bucketJoin(dataA, dataB).as[JoinedDS]

    println("***** Naive Join Plans *****")
    naiveJoinDS.explain()
    /*== Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [url#3, id#0, country#1, ip#2, page_info#18]
       +- SortMergeJoin [url#3], [url#17], Inner
          :- Sort [url#3 ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(url#3, 200), ENSURE_REQUIREMENTS, [plan_id=85]
          :     +- Filter isnotnull(url#3)
          :        +- FileScan parquet [id#0,country#1,ip#2,url#3] Batched: true, DataFilters: [isnotnull(url#3)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/rubybear/sovrn/datasets/dataA], PartitionFilters: [], PushedFilters: [IsNotNull(url)], ReadSchema: struct<id:string,country:string,ip:string,url:string>
          +- Sort [url#17 ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(url#17, 200), ENSURE_REQUIREMENTS, [plan_id=86]
                +- Filter isnotnull(url#17)
                   +- FileScan parquet [url#17,page_info#18] Batched: true, DataFilters: [isnotnull(url#17)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/rubybear/sovrn/datasets/dataB], PartitionFilters: [], PushedFilters: [IsNotNull(url)], ReadSchema: struct<url:string,page_info:string>
*/

    println("***** Bucketed Join Plans *****")
    bucketJoinDS.explain()
    /*== Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [url#53, id#50, country#51, ip#52, page_info#59]
       +- SortMergeJoin [url#53], [url#58], Inner
          :- Sort [url#53 ASC NULLS FIRST], false, 0
          :  +- Filter isnotnull(url#53)
          :     +- FileScan parquet spark_catalog.default.dataa[id#50,country#51,ip#52,url#53] Batched: true, Bucketed: true, DataFilters: [isnotnull(url#53)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/mnt/c/Users/ruben/Documents/Dev/Sovrn/spark-warehouse/dataa], PartitionFilters: [], PushedFilters: [IsNotNull(url)], ReadSchema: struct<id:string,country:string,ip:string,url:string>, SelectedBucketsCount: 10 out of 10
          +- Sort [url#58 ASC NULLS FIRST], false, 0
             +- Filter isnotnull(url#58)
                +- FileScan parquet spark_catalog.default.datab[url#58,page_info#59] Batched: true, Bucketed: true, DataFilters: [isnotnull(url#58)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/mnt/c/Users/ruben/Documents/Dev/Sovrn/spark-warehouse/datab], PartitionFilters: [], PushedFilters: [IsNotNull(url)], ReadSchema: struct<url:string,page_info:string>, SelectedBucketsCount: 10 out of 10
                */

    bucketJoinDS.write.mode("Overwrite").parquet(joinedDataPath)

  }

  def naiveJoin(dataA: Dataset[DataA], dataB: Dataset[DataB]) = {
    dataA.join(dataB, Seq(URL))
  }

  def bucketJoin(dataA: Dataset[DataA], dataB: Dataset[DataB])(implicit spark: SparkSession): DataFrame = {
    //Need to bucket and save as table
    saveAsTable(dataA, "dataA")
    saveAsTable(dataB, "dataB")

    //read from table
    val dataTableA = spark.read.table("dataA")
    val dataTableB = spark.read.table("dataB")

    dataTableA.join(dataTableB, Seq(URL))
  }

  def saveAsTable[T](data: Dataset[T], tableName: String)(implicit spark: SparkSession): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    data.write.mode("Overwrite").bucketBy(BUCKETS, URL).sortBy("url").saveAsTable(tableName)
  }

}
