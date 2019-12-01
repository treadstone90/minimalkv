package com.treadstone90.mkvstore

// and when we want we can read it back by looking at the offset where this chunk is written.
case class Needle(size: Int, data: Array[Byte])

object Needle {
  // Key format
  /**
   * size of key(4 bytes) | key bytes | size of value (n bytes)  | value bytes | checksum
   */
  // 16 bytes for key
  // 4 bytes for integer.
  // 8 bytes since we use crc32

  val KeySize = 16
  val ChecksumSize = 8

  def getTotalBytes(keySize: Int, valueSize: Int): Int = {
    4 + keySize + 4 + 8 + valueSize
  }
}