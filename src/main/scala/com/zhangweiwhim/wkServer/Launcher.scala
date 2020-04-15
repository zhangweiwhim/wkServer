package com.zhangweiwhim.wkServer


import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.launcher.SparkLauncher
import java.io.IOException

/**
 * Description: wkServer
 * Created by zhangwei on 2020/4/9 11:09
 */
object Launcher {
  @throws[IOException]
  def launch():String = {
    var result = "0000"
    val handler = new SparkLauncher()
      .setAppName("wk")
//      .setSparkHome("/opt/cloudera/parcels/CDH/lib/spark")
      .setSparkHome("/opt/cloudera/parcels/SPARK2/lib/spark2")
      .setMaster("yarn")
      .setAppResource("/root/wk/wk_batch-0.1.jar")
      .setMainClass("com.zhangweiwhim.wkServer.RepayCalc")
      .addAppArgs("I come from Launcher")
      .setDeployMode("cluster")
      .startApplication(new SparkAppHandle.Listener() {
        override def stateChanged(handle: SparkAppHandle): Unit = {
          println("**********  state  changed  **********")
        }

        override
        def infoChanged(handle: SparkAppHandle): Unit = {
          println("**********  info  changed  **********")
        }
      })

    import scala.util.control._
    val loop = new Breaks
    loop.breakable {
      while (true) {
        if ("FINISHED".equalsIgnoreCase(handler.getState.toString)) {
          result = "0000"
          loop.break
        } else if ("FAILED".equalsIgnoreCase(handler.getState.toString)) {
          result = "9999"
          loop.break
        }
      }
    }
    result
  }
}
