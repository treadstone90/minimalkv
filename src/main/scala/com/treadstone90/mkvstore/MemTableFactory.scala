package com.treadstone90.mkvstore

import java.nio.ByteBuffer

trait MemTableFactory[T] {
  def emptyMemTable: MemTable[T]
  def fromLogStream(iterator: Iterator[LogEntry]): MemTable[T]
}

class MemTableFactoryImpl[T](dbPath: String, sliceable: Sliceable[T]) extends MemTableFactory[T] {
  def emptyMemTable: MemTable[T] = {
    new MemTableImpl[T](sliceable)
  }

  def fromLogStream(iterator: Iterator[LogEntry]): MemTable[T] = {
    val memTable = emptyMemTable
    while(iterator.hasNext) {
      val logEntry = iterator.next()
      logEntry match {
        case WriteLogEntry(_, key, sequenceId, _, _) =>
          val k = sliceable.fromByteBuffer(ByteBuffer.wrap(key))
          memTable.put(InternalKey[T](sequenceId, k), logEntry)
        case DeleteLogEntry(_, key, sequenceId) =>
          val k = sliceable.fromByteBuffer(ByteBuffer.wrap(key))
          memTable.delete(InternalKey[T](sequenceId, k), logEntry)
      }
    }
    memTable
  }
}