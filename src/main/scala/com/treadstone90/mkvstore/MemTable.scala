package com.treadstone90.mkvstore

import scala.collection.convert.AsScalaConverters

trait MemTable[T] {
  def get(internalKey: InternalKey[T]): Option[LogEntry]

  def keys: Iterable[InternalKey[T]]

  def entries: Iterator[(InternalKey[T], LogEntry)]

  def put(internalKey: InternalKey[T], value: Needle): Unit

  def delete(internalKey: InternalKey[T]): Unit

  def size: Int
}

class MemTableImpl[T](writeAheadLog: WriteAheadLog, sliceable: Sliceable[T]) extends MemTable[T] with AsScalaConverters {
  // if memtable stored deleltion as an entry then memtable itsel can decide that key is DELETED.
  // otherwise there is no way to differentiate between deleted key and key NOT in current memtabl.
  // SO you want to put the deletion entry in Memtable.
  // How does this affect the key scheme. Well it does. When I am storing entries i need the latest sequenceId.
  // So because of that I need to sort keys by the natual order and sort sequence Id in descendeing.
  // Or you can read all keys with a certain prefix and then decide. Instead of having a point lookup in the map.
  var memTableEntry = new java.util.TreeMap[InternalKey[T], LogEntry](sliceable.keyOrdering)
  var currentMemTableSize = 0

  def get(internalKey: InternalKey[T]): Option[LogEntry] = {
    Option(memTableEntry.ceilingEntry(internalKey)).find(entry => entry.getKey.key == internalKey.key).map(_.getValue)
  }

  def size: Int = currentMemTableSize

  def put(internalKey: InternalKey[T], value: Needle) = {
    // we need to first write to write ahead log.
    // and then persist to the
    val keyBytes = sliceable.asByteBuffer(internalKey.key).array()
    val logEntry = WriteLogEntry(keyBytes.length, keyBytes, internalKey.sequenceId, value.size, value.data)
    writeAheadLog.appendEntry(logEntry)
    memTableEntry.put(internalKey, logEntry)
    currentMemTableSize += value.size
  }

  def delete(internalKey: InternalKey[T]): Unit = {
    val keyBytes = sliceable.asByteBuffer(internalKey.key).array()
    val logEntry = DeleteLogEntry(keyBytes.length, keyBytes, internalKey.sequenceId)
    writeAheadLog.appendEntry(logEntry)
    memTableEntry.put(internalKey, logEntry)

    // We think a Delete entry holds 4 bytes.
    currentMemTableSize += 10
  }

  def keys: Iterable[InternalKey[T]] = asScalaSet(memTableEntry.keySet())

  def entries: Iterator[(InternalKey[T], LogEntry)] = asScalaSet(memTableEntry.entrySet()).toIterator.map(entry => (entry.getKey, entry.getValue))
}
