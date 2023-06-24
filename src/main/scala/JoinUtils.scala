object JoinUtils {

  case class DataA(id: String, country: String, ip: String = "", url: String)

  case class DataB(url: String, page_info: String = "")

  case class JoinedDS(id: String, country: String, ip: String, url: String, page_info: String)

}
