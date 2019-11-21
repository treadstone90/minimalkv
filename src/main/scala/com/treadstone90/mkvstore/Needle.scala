package com.treadstone90.mkvstore

// so a need is written to a file.
// and when we want we can read it back by looking at the offset where this chunk is written.
case class Needle(key: Array[Byte], size: Int, data: Array[Byte])

object Needle {
  // 16 bytes for key
  // 4 bytes for integer.
  // 8 bytes since we use crc32
  val KeySize = 16
  val ChecksumSize = 8
  val MetadataSizeBytes = 28
}
