package com.treadstone90.mkvstore

import java.io.{ByteArrayInputStream, RandomAccessFile}
import java.nio.ByteBuffer

import com.google.common.hash.BloomFilter

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
}