package com.treadstone90.mkvstore

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean


trait MemTableWriter[T] extends Writer[InternalKey[T]] with Closeable {
  def get(internalKey: InternalKey[T]): Option[LogEntry]
  def delete(internalKey: InternalKey[T]): Unit
  def writeBatch(writeBatch: WriteBatch[T])
}

class MemTableWriterImpl[T](SSTableManager: SSTableManager[T],
                            sliceable: Sliceable[T],
                            writeAheadLog: WriteAheadLog[LogEntry],
                            initialMemTable: MemTable[T],
                            memTableFactory: MemTableFactory[T]) extends MemTableWriter[T] {

  var currentWriteMemTable: MemTable[T] = initialMemTable
  var currentReadMemTable: MemTable[T] = initialMemTable
  var oldReadMemTable: Option[MemTable[T]] = None
  var snapshotInProgress = new AtomicBoolean(false)
  var sstCount = 0

  def get(internalKey: InternalKey[T]): Option[LogEntry] = {
    println(s"Reading for $internalKey")
    currentReadMemTable.get(internalKey)
      .orElse(oldReadMemTable.flatMap(_.get(internalKey)))
  }

  def write(internalKey: InternalKey[T], value: Array[Byte]): Unit = {
    val keyBytes = sliceable.asByteBuffer(internalKey.key).array()
    val logEntry = WriteLogEntry(keyBytes.length, keyBytes, internalKey.sequenceId, value.length, value)
    writeAheadLog.appendEntry(logEntry)
    currentWriteMemTable.put(internalKey, logEntry)
    createSSTableIfNecessary()
  }

  def writeBatch(writeBatch: WriteBatch[T]): Unit = {
    val logEntries: Seq[(InternalKey[T], LogEntry)] = writeBatch.operations.map {
      case InternalPutOperation(internalKey, value) =>
        val keyBytes = sliceable.asByteBuffer(internalKey.key).array()
        (internalKey, WriteLogEntry(keyBytes.length, keyBytes, internalKey.sequenceId, value.length, value))

      case InternalDeleteOperation(internalKey) =>
        val keyBytes = sliceable.asByteBuffer(internalKey.key).array()
        (internalKey, DeleteLogEntry(keyBytes.length, keyBytes, internalKey.sequenceId))
    }
    // atomic write to WAL
    writeAheadLog.appendEntries(logEntries.map(_._2))
    logEntries.foreach { case(internalKey, entry) =>
      currentWriteMemTable.put(internalKey, entry)
    }
  }

  def delete(internalKey: InternalKey[T]): Unit = {
    val keyBytes = sliceable.asByteBuffer(internalKey.key).array()
    val logEntry = DeleteLogEntry(keyBytes.length, keyBytes, internalKey.sequenceId)
    writeAheadLog.appendEntry(logEntry)

    currentWriteMemTable.delete(internalKey, logEntry)
    createSSTableIfNecessary()
  }

  private def createSSTableIfNecessary() = {
    if(currentWriteMemTable.size > KVStore.MaxMemTableSize) {
      // set current memtable to old read memtable
      val currentSSTCount = sstCount

      if(sstCount == currentSSTCount && snapshotInProgress.compareAndSet(false, true)) {
        println("Creaitng a new SSTable")
        writeAheadLog.rolloverFile()
        oldReadMemTable = Some(currentWriteMemTable)

        // use new memtables for read and write.
        currentReadMemTable = memTableFactory.emptyMemTable
        currentWriteMemTable = currentReadMemTable

        SSTableManager.writeMemTable(oldReadMemTable.get).onSuccess { _ =>
          oldReadMemTable = None
          sstCount += 1
          snapshotInProgress.set(false)
        }
      }
    }
  }

  def close(): Unit = {
    SSTableManager.close()
  }
}