package com.zhangweiwhim

import org.springframework.boot.SpringApplication

/**
 * Description: wkServer
 * Created by zhangwei on 2020/4/8 20:33
 */

object WkprojApplication  {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[AppConfig])
  }
}
