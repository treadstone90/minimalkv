package com.treadstone90.mkvstore

import scala.collection.convert.AsScalaConverters

trait MemTable[T] {
  def get(internalKey: InternalKey[T]): Option[LogEntry]

  def keys: Iterable[InternalKey[T]]

  def entries: Iterator[(InternalKey[T], LogEntry)]

  def put(internalKey: InternalKey[T], logEntry: LogEntry): Unit

  def delete(internalKey: InternalKey[T], logEntry: LogEntry): Unit

  def size: Int
}

class MemTableImpl[T](sliceable: Sliceable[T]) extends MemTable[T] with AsScalaConverters {
  // if memtable stored deleltion as an entry then memtable itsel can decide that key is DELETED.
  // otherwise there is no way to differentiate between deleted key and key NOT in current memtabl.
  var memTableEntry = new java.util.TreeMap[InternalKey[T], LogEntry](sliceable.keyOrdering)
  var currentMemTableSize = 0

  def get(internalKey: InternalKey[T]): Option[LogEntry] = {
    Option(memTableEntry.ceilingEntry(internalKey))
      .find(entry => entry.getKey.key == internalKey.key)
      .map(_.getValue)
  }

  def size: Int = currentMemTableSize

  def put(internalKey: InternalKey[T], logEntry: LogEntry) = {
    // we need to first write to write ahead log.
    // and then persist to the
    memTableEntry.put(internalKey, logEntry)
    currentMemTableSize += logEntry.logEntrySize
  }

  def delete(internalKey: InternalKey[T], logEntry: LogEntry): Unit = {
    memTableEntry.put(internalKey, logEntry)
    // We think a Delete entry holds 4 bytes.
    currentMemTableSize += 10
  }

  def keys: Iterable[InternalKey[T]] = asScalaSet(memTableEntry.keySet())

  def entries: Iterator[(InternalKey[T], LogEntry)] =
    asScalaSet(memTableEntry.entrySet()).toIterator.map(entry => (entry.getKey, entry.getValue))
}
