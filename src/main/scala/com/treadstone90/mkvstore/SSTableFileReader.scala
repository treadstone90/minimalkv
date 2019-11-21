package com.treadstone90.mkvstore

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.Base64
import java.util.concurrent.ConcurrentSkipListMap
import java.util.zip.CRC32

import com.google.common.base.CharMatcher

object SSTableFileReader {
  /**
   * Read an SSTable file given by filePath and return the SkipList of ByteBuffer to SSTableEntries.
   * @param filePath
   */
  def readFile(filePath: String): ConcurrentSkipListMap[ByteBuffer, SSTableEntryMeta] = {
    val segmentId = filePath.split("-") match {
      case Array(_, number) => CharMatcher.is('0').trimLeadingFrom(number).toInt
      case _ => throw new RuntimeException("Not a valid SST file name")
    }
    val randomAccessFile = new RandomAccessFile(filePath, "r")
    val map = new ConcurrentSkipListMap[ByteBuffer, SSTableEntryMeta]()

    while(randomAccessFile.getFilePointer < randomAccessFile.length()) {
      val startOffset = randomAccessFile.getFilePointer
      val currentKey = new Array[Byte](Needle.KeySize)
      //read key + size
      randomAccessFile.read(currentKey, 0, Needle.KeySize)
      val currentKeyBuf = ByteBuffer.wrap(currentKey)
      val size = randomAccessFile.readInt()
      val dataByteArray = new Array[Byte](size)
      randomAccessFile.read(dataByteArray)
      val checkSum = randomAccessFile.readLong()
      map.put(currentKeyBuf, SSTableEntryMeta(size, startOffset, checkSum, segmentId))
    }
    map
  }

  def printSSTFileContents(filePath: String): Unit = {
    val randomAccessFile = new RandomAccessFile(filePath, "r")
    while(randomAccessFile.getFilePointer < randomAccessFile.length()) {
      val currentKey = new Array[Byte](Needle.KeySize)
      //read key + size
      randomAccessFile.read(currentKey, 0, Needle.KeySize)
      val size = randomAccessFile.readInt()
      val dataByteArray = new Array[Byte](size)
      randomAccessFile.read(dataByteArray)
      val checkSumInFile = randomAccessFile.readLong()
      println(Base64.getEncoder.encodeToString(currentKey)  + " " + Base64.getEncoder.encodeToString(dataByteArray))
      val checkSum = new CRC32()
      checkSum.update(dataByteArray)
      assert(checkSumInFile == checkSum.getValue)
    }
  }

  def main(args: Array[String]): Unit = {
    printSSTFileContents("/tmp/blockStore/part-0")
  }
}
