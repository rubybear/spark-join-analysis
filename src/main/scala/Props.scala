trait Props {
  val BUCKETS: Int = sys.env.getOrElse("bucket_num", "10").toInt
  val URL: String = "URL"

  //These data locations should be a properly formatted s3 string such as: "s3://your-bucket/dataA.parquet"
  val dataAPath: String = sys.env.getOrElse("data_a_location", "")
  val dataBPath: String = sys.env.getOrElse("data_b_location", "")

  val joinedDataPath: String = sys.env.getOrElse("joined_data_output_location", "")

}
