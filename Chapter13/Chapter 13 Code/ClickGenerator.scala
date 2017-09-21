package spark.ml.cookbook.chapter13

import java.time.LocalDateTime
import scala.util.Random._

case class ClickEvent(userId: String, ipAddress: String, time: String, url: String, statusCode: String)

object ClickGenerator {

  val statusCodeData = Seq(200, 404, 500)
  val urlData = Seq("http://www.fakefoo.com",
    "http://www.fakefoo.com/downloads",
    "http://www.fakefoo.com/search",
    "http://www.fakefoo.com/login",
    "http://www.fakefoo.com/settings",
    "http://www.fakefoo.com/news",
    "http://www.fakefoo.com/reports",
    "http://www.fakefoo.com/images",
    "http://www.fakefoo.com/css",
    "http://www.fakefoo.com/sounds",
    "http://www.fakefoo.com/admin",
    "http://www.fakefoo.com/accounts"
  )
  val ipAddressData = generateIpAddress()
  val timeStampData = generateTimeStamp()
  val userIdData = generateUserId()

  def generateIpAddress(): Seq[String] = {
    for (n <- 1 to 255) yield s"127.0.0.$n"
  }

  def generateTimeStamp(): Seq[String] = {
    val now = LocalDateTime.now()
    for (n <- 1 to 1000) yield LocalDateTime.of(now.toLocalDate,
      now.toLocalTime.plusSeconds(n)).toString
  }

  def generateUserId(): Seq[Int] = {
    for (id <- 1 to 1000) yield id
  }

  def generateClicks(clicks: Int = 1): Seq[String] = {
     0.until(clicks).map(i => {
       val statusCode = statusCodeData(nextInt(statusCodeData.size))
       val ipAddress = ipAddressData(nextInt(ipAddressData.size))
       val timeStamp = timeStampData(nextInt(timeStampData.size))
       val url = urlData(nextInt(urlData.size))
       val userId = userIdData(nextInt(userIdData.size))

       s"$userId,$ipAddress,$timeStamp,$url,$statusCode"
     })
  }

  def parseClicks(data: String): ClickEvent = {
     val fields = data.split(",")
     new ClickEvent(fields(0), fields(1), fields(2), fields(3), fields(4))
  }

}
