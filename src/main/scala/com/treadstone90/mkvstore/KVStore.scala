package com.treadstone90.mkvstore

import java.nio.ByteBuffer

import scala.collection.convert.AsScalaConverters
import scala.collection.mutable

trait KVStore {
  /**
   * Read a Needle from a KVStore.
   * Needle is an encapsulation of a result.
   * @param key Key is a byte array
   * @return Option[Needle]
   */
  def get(key: Array[Byte]): Option[Needle]

  /**
   *
   * @param needle
   */
  def write(needle: Needle): Unit
}


class KVStoreImpl(ssTableManager: SSTableManager) extends KVStore with AsScalaConverters {
  import KVStore._

  var memTable = new mutable.TreeMap[ByteBuffer, Array[Byte]]()
  var currentMemTableSize = 0

  /**
   *
   * @param key
   * @return
   */
  def get(key: Array[Byte]): Option[Needle] = {
    val keyByteBuffer = ByteBuffer.wrap(key)

    if(memTable.contains(keyByteBuffer)) {
      val data = memTable(keyByteBuffer)
      Some(Needle(key, data.length, data))
    } else {
      ssTableManager.get(key)
    }
  }

  /**
   *
   * @param needle
   */
  def write(needle: Needle): Unit = {
    memTable.put(ByteBuffer.wrap(needle.key), needle.data)
    currentMemTableSize += needle.size

    if(currentMemTableSize > MaxMemTableSize) {
      val temp = memTable
      memTable = new mutable.TreeMap[ByteBuffer, Array[Byte]]()
      currentMemTableSize = 0
      ssTableManager.writeMemTable(temp)
    }
  }
}


object KVStore {
  // store some keys in skipList.
  // find the keys smaller and larger than the key.
  // actually we need one sstableIndex per ssTable file.
  val MaxMemTableSize = 10*1024*1024
}