package com.zhangweiwhim.wkServer

import java.sql.{Connection, DriverManager}
import java.time.LocalDate.now
import java.util.Properties

import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RestController}

/**
 * Description: wkServer
 * Created by zhangwei on 2020/4/8 20:26
 */


@RestController
@RequestMapping(Array("/bigdata"))
class WkServer {
  @RequestMapping(value = Array("/doWk"), method = Array(RequestMethod.GET))
  def doWk(): String = {
    val startDate = now
    val result = Launcher.launch()
    val properties = new Properties()
    val path = this.getClass.getResourceAsStream("/conf.properties") // Thread.currentThread().getContextClassLoader.getResource("/conf.properties").getPath //文件要放到resource文件夹下
    properties.load(path)
    val url = properties.getProperty("mysql.db.url")
    val table = properties.getProperty("mysql.output.table")
    val mysqlUser = properties.getProperty("mysql.user")
    val mysqlPassword = properties.getProperty("mysql.password")
    val endDate =now
    //更新标签
    val conn: Connection = getOnlineConnection(url, mysqlUser, mysqlPassword)
    try {
      conn.setAutoCommit(false)
      val sql = new StringBuilder()
        .append("replace into m_batch (BATCHID,PROCNAME,BATCHNAME,STARTTIME,ENDTIME,BATCHSTATUS,RESULT,DATACOUNT,BATCHWORKCYCLE,PARAMETERS,SORTNO,VALIDFLG) values(?,?,?,?,?,?,?,?,?,?,?,?)")
      val pstm = conn.prepareStatement(sql.toString())
      if(result=="0000"){
        pstm.setInt(6, 0)
        pstm.setString(7, "执行成功。")
      }else{
        pstm.setInt(6, 99)
        pstm.setString(7, "执行失败。")
      }
      pstm.setInt(1, 999)
      pstm.setString(2, "bigdata_wk")
      pstm.setString(3, "大数据端wk逾期表计算")
      pstm.setDate(4, java.sql.Date.valueOf(startDate))
      pstm.setDate(5, java.sql.Date.valueOf(endDate))
      pstm.setInt(8, 0)
      pstm.setString(9, "每日")
      pstm.setString(10, null)
      pstm.setString(11, null)
      pstm.setInt(12, 1)
      pstm.executeBatch()
      pstm.executeUpdate() > 0
      conn.commit()
    }
    finally {
      conn.close()
    }
    result
  }

  def getOnlineConnection(onlineUrl: String, username: String, password: String): Connection = {
    DriverManager.getConnection(onlineUrl, username, password)
  }
}