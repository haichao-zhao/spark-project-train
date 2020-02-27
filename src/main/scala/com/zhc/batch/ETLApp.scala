package com.zhc.batch

import java.util.{Date, Locale}

import com.zhc.domain.{LogInfo, UserAgentInfo}
import com.zhc.utils.UAgentUtils
import com.zhc.utils.ip.IPParser
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ETLApp {

  def log_ETL(rdd: RDD[String], spark: SparkSession) = {


    import spark.implicits._
    import org.apache.spark.sql.functions._
    def formatTime() = udf((time: String) => {
      FastDateFormat.getInstance("yyyyMMddHHmm").format(
        new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
          .parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
        ))
    })

    var logDF = rdd.map(x => {
      val strings: Array[String] = x.split(" ")

      val ip: String = strings(0)

      var country: String = "unknown"
      var province: String = "unknown"
      var city: String = "unknown"


      val ipParse: IPParser = IPParser.getInstance
      if (StringUtils.isNotBlank(ip)) {
        val info: IPParser.RegionInfo = ipParse.analyseIp(ip)

        if (null != info.getCountry){
          country = info.getCountry
        }
        if (null != info.getProvince){
          province = info.getProvince
        }
        if (null != info.getCity){
          city = info.getCity
        }
      }

      val time: String = strings(3) + " " + strings(4)


      val method: String = strings(5).replace("\"", "")
      val url: String = strings(6)
      val protocal: String = strings(7).replace("\"", "")


      val status: String = strings(8)
      val bytessent: String = strings(9)
      val referer: String = strings(11)
      val ua: String = x.split("\"")(7)

      val info: UserAgentInfo = UAgentUtils.getUserAgentInfot(ua)

      val browsername: String = info.getBrowserName
      val browserversion: String = info.getBrowserVersion
      val osname: String = info.getOsName
      val osversion: String = info.getOsVersion

      new LogInfo(ip, country, province, city, time, method, url, protocal, status, bytessent, referer, ua, browsername, browserversion, osname, osversion)
    }).toDF()

    // 在已有的DF之上添加或者修改字段
    logDF = logDF.withColumn("formattime", formatTime()(logDF("time")))

    logDF
  }
}
