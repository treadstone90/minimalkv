package com.treadstone90.mkvstore

trait MemTableWriter[T] {
  def get(internalKey: InternalKey[T]): Option[LogEntry]

  def put(internalKey: InternalKey[T], value: Needle): Unit

  def delete(internalKey: InternalKey[T]): Unit
}

class MemTableWriterImpl[T](SSTableManager: SSTableManager[T],
                            sliceable: Sliceable[T],
                            memTableFactory: MemTableFactory[T]) extends MemTableWriter[T] {

  var currentWriteMemTable: MemTable[T] = memTableFactory.emptyMemTable
  var currentReadMemTable: MemTable[T] = currentWriteMemTable
  var oldReadMemTable: Option[MemTable[T]] = None

  def get(internalKey: InternalKey[T]): Option[LogEntry] = {
    println(s"Reading for $internalKey")
    currentReadMemTable.get(internalKey)
      .orElse(oldReadMemTable.flatMap(_.get(internalKey)))
  }

  def put(internalKey: InternalKey[T], value: Needle): Unit = {
    currentWriteMemTable.put(internalKey, value)
    createSSTableIfNecessary()
  }

  def delete(internalKey: InternalKey[T]): Unit = {
    currentWriteMemTable.delete(internalKey)
    createSSTableIfNecessary()
  }

  private def createSSTableIfNecessary(): Unit = {
    if(currentWriteMemTable.size > KVStore.MaxMemTableSize) {
      println("Creaitng a new SSTable")
      // set current memtable to old read memtable
      oldReadMemTable = Some(currentWriteMemTable)

      // use new memtables for read and write.
      currentReadMemTable = memTableFactory.emptyMemTable
      currentWriteMemTable = currentReadMemTable

      SSTableManager.writeMemTable(oldReadMemTable.get)
      oldReadMemTable = None
    }
  }
}