package com.ning.spark

import java.util.concurrent.ConcurrentHashMap

import com.ning.network.IOMode

/**
  * Created by zhaoshufen on 2017/3/12.
  */
class SparkConf {
  private val settings = new ConcurrentHashMap[String, String]()
  def set(key: String,value: String): SparkConf = {
    if(key == null){
      throw new RuntimeException("null key")
    }
    if(value == null){
      throw new RuntimeException("null value")
    }
    settings.put(key,value)
    this
  }
  def get(key: String): String = {
    return getOption(key).getOrElse(throw new NoSuchElementException(key))
  }
  def getOption(key: String): Option[String] = {
    return Option(settings.get(key))
  }
  def ioMode(): IOMode = {

  }
}
