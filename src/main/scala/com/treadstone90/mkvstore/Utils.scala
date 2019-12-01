package com.treadstone90.mkvstore

import java.io.{ByteArrayInputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.zip.CRC32

import com.google.common.hash.BloomFilter
import com.twitter.util.Stopwatch

trait SstUtils[T] {
  def sliceable: Sliceable[T]

  def readHeader(readRandomAccess: RandomAccessFile): Header[T] = {
    val headerSize = readRandomAccess.readInt()
    val bytes = new Array[Byte](headerSize)
    val byteBuffer = ByteBuffer.wrap(bytes)
    readRandomAccess.read(bytes)
    val segmentId = byteBuffer.getInt
    val bloomFilterSize = byteBuffer.getInt
    val bis = new ByteArrayInputStream(byteBuffer.array(), byteBuffer.position(), bloomFilterSize)
    val bloomFilter = BloomFilter.readFrom[T](bis, sliceable.funnel)
    bis.close()
    Header(bloomFilter, segmentId)
  }

  def writeRecord(key: T, value: Array[Byte], randomAccessFile: RandomAccessFile): SSTableEntryMeta = {
    val startOffset = randomAccessFile.getFilePointer
    val keyBytes = sliceable.asByteBuffer(key).array()

    val size = Needle.getTotalBytes(keyBytes.length, value.length)
    val byteBuffer = ByteBuffer.allocate(size)
    val checkSum = new CRC32()
    checkSum.update(value)

    byteBuffer.putInt(keyBytes.length)
    byteBuffer.put(keyBytes)
    byteBuffer.putInt(value.length)
    byteBuffer.put(value)
    byteBuffer.putLong(checkSum.getValue)
    randomAccessFile.write(byteBuffer.array())

    SSTableEntryMeta(value.length, startOffset, checkSum.getValue)
  }

  def writeKvRecord(record: Record, randomAccessFile: RandomAccessFile): SSTableEntryMeta = {
    val startOffset = randomAccessFile.getFilePointer
    //
    val byteBuffer = ByteBuffer.allocate(record.size + 4 + 8)
    byteBuffer.putInt(record.size)
    byteBuffer.putLong(record.checkSum)
    byteBuffer.put(record.data)
    randomAccessFile.write(byteBuffer.array())

    SSTableEntryMeta(record.size, startOffset, record.checkSum)
  }

  def readRecord(readRandomAccess: RandomAccessFile): Record = {
    val recordSize = readRandomAccess.readInt()
    val checkSum = readRandomAccess.readLong()
    val dataByteArray = new Array[Byte](recordSize)
    readRandomAccess.read(dataByteArray)
    Record(recordSize, checkSum, dataByteArray)
  }

//  def readRecord(readRandomAccess: RandomAccessFile): (T, Needle) = {
//    val keySize = readRandomAccess.readInt()
//    val keyBytes = new Array[Byte](keySize)
//    readRandomAccess.read(keyBytes)
//    val key = sliceable.fromByteBuffer(ByteBuffer.wrap(keyBytes))
//    val valueSize = readRandomAccess.readInt()
//    val valueBytes = new Array[Byte](valueSize)
//    readRandomAccess.read(valueBytes)
//    val checkSum = readRandomAccess.readLong()
//    val crc32 = new CRC32()
//    crc32.update(valueBytes)
//    assert(checkSum == crc32.getValue)
//    (key, Needle(valueSize, valueBytes))
//  }
}

trait Utils {
  def time[T](label: String)(f: => T): T = {
    val elapsed = Stopwatch.start()
    val result: T = f
    val end = elapsed()
    println(s"[$label] took $end")
    result
  }
}
