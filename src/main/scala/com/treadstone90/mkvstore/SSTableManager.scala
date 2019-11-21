package com.treadstone90.mkvstore

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import java.util.zip.CRC32

import com.google.common.hash.{BloomFilter, Funnels}

import scala.collection.convert.AsScalaConverters
import scala.collection.mutable

case class SSTableEntryMeta(size: Int, offset: Long, checkSum: Long, ssTableIndex: Int)

trait SSTableManager {
  def get(key: Array[Byte]): Option[Needle]
  def writeMemTable(memTable: mutable.TreeMap[ByteBuffer, Array[Byte]])
}

class SSTableManagerImpl(dbPath: String) extends SSTableManager with AsScalaConverters {
  val ssTableIndices = new mutable.LinkedHashMap[Int, ConcurrentSkipListMap[ByteBuffer, SSTableEntryMeta]]()
  val ssTableBloomFilter = new mutable.HashMap[Int, BloomFilter[Array[Byte]]]()

  val ssTableFiles = new ConcurrentHashMap[Int, RandomAccessFile]()
  var CurrentSSTableSegment = 0
  // every 10KB we write out an index to the SSTable.
  val KeyInterval: Long = 10L*1024

  def close(): Unit = {
    mapAsScalaMap(ssTableFiles).foreach(_._2.close())
  }

  def get(key: Array[Byte]): Option[Needle] = {
    val x = ssTableIndices.flatten { case(segmentId, skipListMap) =>
      if(ssTableBloomFilter(segmentId).mightContain(key)) {
        checkInSSTable(key, skipListMap)
      } else {
        None
      }
    }
    x.flatMap(meta => readFromFile(key, meta)).headOption
  }

  def writeMemTable(memTable: mutable.TreeMap[ByteBuffer, Array[Byte]]): Unit = {
    val ssTableIndexMap = new ConcurrentSkipListMap[ByteBuffer, SSTableEntryMeta]()
    val bloomFilter = BloomFilter.create[Array[Byte]](Funnels.byteArrayFunnel(), memTable.size)
    memTable.keys.foreach(buf => bloomFilter.put(buf.array()))
    var currentWritten = 0L

    memTable.foreach { case(key, value) =>
      val metadata = writeNeedle(key.array(), value, CurrentSSTableSegment)
      if (currentWritten == 0 || currentWritten > KeyInterval) {
        ssTableIndexMap.put(key, metadata)
        currentWritten = 0L
      }
      currentWritten += metadata.size
    }

    ssTableBloomFilter.put(CurrentSSTableSegment, bloomFilter)
    ssTableIndices.put(CurrentSSTableSegment, ssTableIndexMap)
    CurrentSSTableSegment += 1
  }

  private def checkInSSTable(key: Array[Byte],
                             skipList: ConcurrentSkipListMap[ByteBuffer, SSTableEntryMeta]): Option[SSTableEntryMeta] = {
    val byteBuffer = ByteBuffer.wrap(key)
    val lastEntry = skipList.lastEntry()
    val smallerEntry = Option(skipList.floorEntry(byteBuffer))


    smallerEntry match {
      case None => None
      case Some(e) if e.getKey == byteBuffer =>
        Some(e.getValue)
      case Some(e) if e.getKey == lastEntry.getKey =>
        None
      case Some(e) =>
        Some(e.getValue)
    }
  }

  private def readFromFile(key: Array[Byte], ssTableMeta: SSTableEntryMeta): Option[Needle] = {
    val readRandomAccessFile = ssTableFiles.get(ssTableMeta.ssTableIndex)
    val keyByteBuffer = ByteBuffer.wrap(key)
    var needleOpt: Option[Needle] = None
    var canStop = false
    readRandomAccessFile.seek(ssTableMeta.offset)

    while(needleOpt.isEmpty && !canStop && readRandomAccessFile.getFilePointer < readRandomAccessFile.length()) {
      val currentKey = new Array[Byte](Needle.KeySize)
      //read key + size
      readRandomAccessFile.read(currentKey, 0, Needle.KeySize)
      val currentKeyBuf = ByteBuffer.wrap(currentKey)
      val size = readRandomAccessFile.readInt()

      if (currentKeyBuf == keyByteBuffer) {
        val dataByteArray = new Array[Byte](size)
        readRandomAccessFile.read(dataByteArray)
        needleOpt = Some(Needle(currentKey, size, dataByteArray))
      } else if(currentKeyBuf.compareTo(keyByteBuffer) > 0) {
        canStop = true
      } else {
        val nextRead = size + Needle.ChecksumSize
        readRandomAccessFile.seek(readRandomAccessFile.getFilePointer + nextRead)
      }
    }
    needleOpt
  }

  private def getSegmentFile(segmentId: Int): String = {
    s"$dbPath/part-$segmentId"
  }

  private def writeNeedle(key: Array[Byte], value: Array[Byte],
                          ssTableSegmentId: Int): SSTableEntryMeta = {
    val randomAccessFile = ssTableFiles.computeIfAbsent(ssTableSegmentId,
      (segmentId: Int) => new RandomAccessFile(s"${getSegmentFile(segmentId)}", "rwd"))

    val startOffset = randomAccessFile.getFilePointer

    val size = value.length + Needle.MetadataSizeBytes
    val byteBuffer = ByteBuffer.allocate(size)
    val checkSum = new CRC32()
    checkSum.update(value)


    byteBuffer.put(key)
    byteBuffer.putInt(value.length)
    byteBuffer.put(value)
    byteBuffer.putLong(checkSum.getValue)
    randomAccessFile.write(byteBuffer.array())

    SSTableEntryMeta(value.length, startOffset, checkSum.getValue, ssTableSegmentId)
  }
}