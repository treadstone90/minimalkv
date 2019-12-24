package com.treadstone90.mkvstore.app

import java.nio.ByteBuffer

import com.treadstone90.mkvstore.{LogEntryIterator, Sliceable}
import com.twitter.inject.Logging
import com.twitter.inject.app.App

object WALReader extends App with Logging {
  override protected def run(): Unit = {
    val iterator = new LogEntryIterator("/tmp/blockStore")
    val intSliceable = Sliceable.int2Slice
    while(iterator.hasNext) {
      val logEntry = iterator.next()
      info(s"Entry: ${intSliceable.fromByteBuffer(ByteBuffer.wrap(logEntry.key))} " +
        s"${logEntry.sequenceId} " +
        s"${logEntry.operationType}")
    }
  }
}
