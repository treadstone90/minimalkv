package com.treadstone90.mkvstore.app

import com.treadstone90.mkvstore.SSTableFactoryImpl
import com.twitter.inject.Logging
import com.twitter.inject.app.App

object MemTableApp extends App with Logging {
  override def run(): Unit = {
    val factory = new SSTableFactoryImpl[Int]("/tmp/blockStore")
    val memTable = factory.fromFile(3)
    info(memTable.get(1044485))
  }
}
