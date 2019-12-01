package com.treadstone90.mkvstore

import java.io.{ByteArrayOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap

import com.google.common.hash.BloomFilter

import scala.collection.convert.AsScalaConverters

trait SSTableFactory[T] {
  def fromMemTable(memTable: MemTable[T], segmentId: Int): SSTable[T]
  def fromFile(segmentId: Int): SSTable[T]
}

/***
 * This class is thread safe and there is no reason for it to not be accessble from multiple threads.
 * At the same we would ideally never need multiple threads here.
 * @param sliceable
 * @tparam T
 */
class SSTableFactoryImpl[T](dbPath: String,
                            val sliceable: Sliceable[T])
  extends SSTableFactory[T]
    with SstUtils[T]
    with AsScalaConverters {

  def writeHeader(bloomFilter: BloomFilter[T], segmentId: Int,
                  writeRandomAccessFile: RandomAccessFile): Int = {

    val bfByteArrayOp = new ByteArrayOutputStream()
    bloomFilter.writeTo(bfByteArrayOp)

    val headerSize = SSTable.headerSize(bfByteArrayOp.size())
    // allocate 4 more bytes for the size of the header.
    val headerByteBuffer = ByteBuffer.allocate(headerSize + 4)

    // THis is the RecordIO for the header.
    // header size + Header.
    // Writes out the headerSize to buf
    headerByteBuffer.putInt(headerSize)

    // writes out the header to buf
    headerByteBuffer.putInt(segmentId)
    headerByteBuffer.putInt(bfByteArrayOp.size())
    headerByteBuffer.put(bfByteArrayOp.toByteArray)

    // writes out headerBuf to file.
    writeRandomAccessFile.write(headerByteBuffer.array())
    headerSize
  }

  def fromMemTable(memTable: MemTable[T], segmentId: Int): SSTable[T] = {
    val ssTableIndexMap = new ConcurrentSkipListMap[T, SSTableEntryMeta](sliceable.ordering)
    val bloomFilter = BloomFilter.create[T](sliceable.funnel, memTable.size)
    memTable.keys.foreach(buf => bloomFilter.put(buf.key))

    val ssTableFile = getSSTFile(segmentId)
    val writeRandomAccessFile = new RandomAccessFile(ssTableFile, "rwd")

    val headerSize = writeHeader(bloomFilter, segmentId, writeRandomAccessFile)
    // TODO this can be part of the footer. and if it is a fixed length footer we can

    var currentWritten = 0L

    memTable.entries.foreach { case(k, value) =>
      println(s"writing ${k.key}")
      val metadata = writeKvRecord(value.serializeToBytes, writeRandomAccessFile)

      if (currentWritten == 0 || currentWritten > SSTableManager.KeyInterval) {
        ssTableIndexMap.put(k.key, metadata)
        currentWritten = 0L
      }
      currentWritten += metadata.size
    }

    println("Writing summary file")
    writeSummaryFile(ssTableIndexMap, segmentId)
    writeRandomAccessFile.close()

    val readRandomAccessFile = new RandomAccessFile(ssTableFile, "r")
    readRandomAccessFile.seek(headerSize)

    new SSTableImpl[T](ssTableIndexMap, segmentId, bloomFilter, sliceable, readRandomAccessFile)
  }


  private def getSSTFile(segmentId: Int): String = {
    s"$dbPath/part-$segmentId.sst"
  }

  private def getSummaryFile(segmentId: Int): String = {
    s"$dbPath/part-$segmentId.summary"
  }


  def writeSummaryFile(concurrentSkipListMap: ConcurrentSkipListMap[T, SSTableEntryMeta],
                       segmentId: Int): Unit = {
    val summaryFile = new RandomAccessFile(getSummaryFile(segmentId), "rwd")
    mapAsScalaConcurrentMap(concurrentSkipListMap).foreach { case(key, value) =>
      writeSummaryRecord(key, value, summaryFile)
    }
    summaryFile.close()
  }

  def writeSummaryRecord(key: T, meta: SSTableEntryMeta,
                  writeRandomAccessFile: RandomAccessFile): Unit = {
    val byteBuffer = sliceable.asByteBuffer(key)
    writeRandomAccessFile.writeInt(byteBuffer.array().length)
    writeRandomAccessFile.write(byteBuffer.array())
    writeRandomAccessFile.write(meta.size)
    writeRandomAccessFile.writeLong(meta.offset)
    writeRandomAccessFile.writeLong(meta.checkSum)
  }

  def readSummaryFile(filePath: String): ConcurrentSkipListMap[T, SSTableEntryMeta] = {
    val readRandomAccessFile = new RandomAccessFile(filePath, "")
    val map = new ConcurrentSkipListMap[T, SSTableEntryMeta](sliceable.ordering)
    val bytes = Array[Byte]()
    readRandomAccessFile.read(bytes)
    val byteBuffer = ByteBuffer.wrap(bytes)

    while(byteBuffer.hasRemaining) {
      val keySize = byteBuffer.getInt
      val keyBytes = new Array[Byte](keySize)
      byteBuffer.get(keyBytes)
      val size = byteBuffer.getInt()
      val offset = byteBuffer.getLong
      val checkSum = byteBuffer.getLong
      map.put(sliceable.fromByteBuffer(byteBuffer), SSTableEntryMeta(size, offset, checkSum))
    }
    readRandomAccessFile.close()
    map
  }

  def fromFile(segmentId: Int): SSTable[T] = {
    val sstFile = getSSTFile(segmentId)
    val randomAccessFile = new RandomAccessFile(sstFile, "r")
    val metadataMap = readSummaryFile(getSummaryFile(segmentId))
    val header = readHeader(randomAccessFile)
    new SSTableImpl[T](metadataMap, header.segmentId, header.bloomFilter, sliceable, randomAccessFile)
  }
}

