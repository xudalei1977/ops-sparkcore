package com.lavapm.utils


/**
  * Http operation
  * Created by ryan on 24/12/2018
  */
object Http {

  val partialUrl = "http://192.168.0.99:8070/solr/findIpDetails?ip="

  /**
    * @param ip The ip for complete the URL to connect to.
    * @param connectTimeout Sets a specified timeout value, in milliseconds,
    *                       to be used when opening a communications link to the resource referenced
    *                       by this URLConnection. If the timeout expires before the connection can
    *                       be established, a java.net.SocketTimeoutException
    *                       is raised. A timeout of zero is interpreted as an infinite timeout.
    *                       Defaults to 5000 ms.
    * @param readTimeout If the timeout expires before there is data available
    *                    for read, a java.net.SocketTimeoutException is raised. A timeout of zero
    *                    is interpreted as an infinite timeout. Defaults to 5000 ms.
    * @param requestMethod Defaults to "GET". (Other methods have not been tested.)
    */
  @throws(classOf[java.io.IOException])
  @throws(classOf[java.net.SocketTimeoutException])
  def get(ip: String,
          connectTimeout: Int = 5000,
          readTimeout: Int = 5000,
          requestMethod: String = "GET"): String = {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(partialUrl+ip.trim)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream,"utf-8").mkString
    if (inputStream != null) inputStream.close

    content
  }

  def sendGet(ip: String): String = {
    try {
      val content = get(ip)
      return content

    } catch {
      case ioe: java.io.IOException => print("error in Http sendGet ioe: " + ioe.toString)
      case ste: java.net.SocketTimeoutException => print("error in Http sendGet ste: " + ste.toString)
    }
    ""
  }

  def main(args: Array[String]): Unit = {
    println(Http.get("111.174.40.184"))
  }
}