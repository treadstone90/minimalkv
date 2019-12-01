package com.treadstone90.mkvstore

import java.io.RandomAccessFile
import java.nio.ByteBuffer


class SSTFileReader[T](val sliceable: Sliceable[T]) extends SstUtils[T] {
  def sstFilePath(filePath: String, printHeader: Boolean): Unit = {
    val readRandomAcceessFile = new RandomAccessFile(filePath, "r")
    val header = readHeader(readRandomAcceessFile)
    var current = 0
    while(readRandomAcceessFile.getFilePointer < readRandomAcceessFile.length()) {
      val record = readRecord(readRandomAcceessFile)
      val entry = LogEntry.deserializeFromRecord(record)

      println(sliceable.fromByteBuffer(ByteBuffer.wrap(entry.key)) + " " +  entry.sequenceId + " " + entry.operationType)
      current += 1
    }
  }
}