package com.treadstone90.mkvstore.app

import com.treadstone90.mkvstore.SSTableFactoryImpl
import com.twitter.inject.Logging
import com.twitter.inject.app.App

object SSTableReader extends App with Logging {
  override def run(): Unit = {
    val factory = new SSTableFactoryImpl[Int]("/tmp/blockStore")
    val ssTable1 = factory.fromFile(0)
    val ssTable2 = factory.fromFile(2)

    val ssTable3 = factory.mergeSSTables(ssTable1, ssTable2, 13)

    val sst3Keys = ssTable3.iterator.map(entry => factory.sliceable.fromByteArray(entry.key)).toList.sorted
    val sst2Keys = ssTable2.iterator.map(entry => factory.sliceable.fromByteArray(entry.key)).toList.sorted
    val sst1Keys = ssTable1.iterator.map(entry => factory.sliceable.fromByteArray(entry.key)).toList.sorted

    info(sst3Keys)
    info(sst2Keys)
    info(sst1Keys)
    info(sst1Keys.union(sst2Keys).diff(sst3Keys))
  }
}
