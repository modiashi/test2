package com.ppg.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by 491620 on 1/30/2018.
  */
object AppConfig {

  val appConfig = ConfigFactory.load("application.conf")

  def getConfig(moduleName: String): Config = {

    val config = appConfig.getConfig(moduleName).withFallback(appConfig.getConfig("common"))
    config
  }

  def loadConfig(fileName: String): Config = {

    ConfigFactory.load(fileName)

  }

  def hasProp(config: Config,  prop: String) : Boolean = {
    try {
      config.hasPath(prop)
    } catch{
      case e: Exception => false
    }

  }

}