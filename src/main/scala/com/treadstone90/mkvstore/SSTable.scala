package com.treadstone90.mkvstore

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap

import com.google.common.hash.BloomFilter

// so we need to know what an SSTable is
//
trait SSTable[T] {
  /**
   * Check if key exists in an SSTable
   * @param key Key
   * @return Option of Needle.
   */
  def get(key: T): Option[LogEntry]
}

case class Header[T](bloomFilter: BloomFilter[T], segmentId: Int)

class SSTableImpl[T](val skipList: ConcurrentSkipListMap[T, SSTableEntryMeta],
                     segmentId: Int,
                     bloomFilter: BloomFilter[T],
                     sliceable: Sliceable[T],
                     readRandomAccessFile: RandomAccessFile) extends SSTable[T] {

  def get(key: T): Option[LogEntry] = {
    if(bloomFilter.mightContain(key)) {
      checkInSSTable(key).flatMap(meta => readFromFile(key, meta))
    } else {
      println("Bloom filter miss")
      None
    }
  }

  def close(): Unit = {
    readRandomAccessFile.close()
  }

  private def checkInSSTable(key: T): Option[SSTableEntryMeta] = {
    val lastEntry = skipList.lastEntry()
    val smallerEntry = Option(skipList.floorEntry(key))

    smallerEntry match {
      case None => None
      case Some(e) if e.getKey == key =>
        println(s"Exact match found for for $key")
        Some(e.getValue)
      case Some(e) if e.getKey == lastEntry.getKey =>
        None
      case Some(e) =>
        println(s"approx match found for  $key")
        Some(e.getValue)
    }
  }

  private def readFromFile(key: T, ssTableMeta: SSTableEntryMeta): Option[LogEntry] = {
    var needleOpt: Option[LogEntry] = None
    var canStop = false
    readRandomAccessFile.seek(ssTableMeta.offset)

    while(needleOpt.isEmpty && !canStop && readRandomAccessFile.getFilePointer < readRandomAccessFile.length()) {
      println(readRandomAccessFile.getFilePointer)
      val keySize = readRandomAccessFile.readInt()
      val checkSum = readRandomAccessFile.readLong()

      val dataByteArray = new Array[Byte](keySize)
      readRandomAccessFile.read(dataByteArray)
      val record = Record(keySize, checkSum, dataByteArray)

      val entry = LogEntry.deserializeFromRecord(record)
      val currentKey = sliceable.fromByteBuffer(ByteBuffer.wrap(entry.key))

      if (currentKey == key) {
        needleOpt = Some(entry)
      } else if(sliceable.ordering.compare(currentKey, key) > 0) {
        canStop = true
      }
    }

    needleOpt
  }
}

object SSTable {
  val BloomFilterLengthBytes = 4
  val SegmentIdBytes = 4
  def headerSize(bloomFilterSize: Int): Int = {
    SegmentIdBytes + BloomFilterLengthBytes + bloomFilterSize
  }
}


