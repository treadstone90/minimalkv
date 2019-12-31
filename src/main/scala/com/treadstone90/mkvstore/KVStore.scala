package com.treadstone90.mkvstore

import java.util.concurrent.atomic.AtomicLong

import com.google.inject.Inject

import scala.collection.convert.AsScalaConverters


trait Writer[K] {
  def write(key: K, value: Array[Byte]): Unit
}

/**
 * Stop the KV Store with write ahead log and compaction.
 * Like enough of this. Then I want you to focus on
 * replication and sharding. and consensus.
 * @tparam K
 */
trait KVStore[K] extends Writer[K] {
  /**
   * Read a Needle from a KVStore.
   * Needle is an encapsulation of a result.
   * @param key Key is a byte array
   * @return Option[Needle]
   */
  def get(key: K): Array[Byte]
  def writeBatch(writeBatch: WriteBatch[K]): Unit
  def delete(key: K): Unit
  def memTableWriter: MemTableWriter[K]
  def ssTableManager: SSTableManager[K]
}

case class InternalKey[T](sequenceId: Long, key: T)
// so we need to take a key type to a byte array
// then we are good.
// The treemaps needs an ordering of this byte array. hmm.
class KVStoreImpl[T]@Inject() (val ssTableManager: SSTableManager[T],
                               val memTableWriter: MemTableWriter[T],
                               initialSequenceNumber: Long,
                               sliceable: Sliceable[T]) extends KVStore[T] with AsScalaConverters {
  val sequenceNumber = new AtomicLong(initialSequenceNumber)

  /**
   * Read a Needle from a KVStore.
   * Needle is an encapsulation of a result.
   *
   * @param key Key is a byte array
   * @return Option[Needle]
   */
  def get(key: T): Array[Byte] = {
    memTableWriter.get(InternalKey(Long.MaxValue, key)).orElse(ssTableManager.get(key)) match {
      case Some(WriteLogEntry(_, _, _, _, value)) => value
      case _ => Array.empty[Byte]
    }
  }

  def write(key: T, value: Array[Byte]): Unit = {
    // we need to first write to write ahead log.
    // and then persist to the
    val internalKey = InternalKey(sequenceNumber.incrementAndGet(), key)
    memTableWriter.write(internalKey, value)
  }

  def writeBatch(writeBatch: WriteBatch[T]): Unit = {
    val internalOps: Seq[Operation[T]] = writeBatch.operations.map {
      case PutOperation(key, value) =>
        InternalPutOperation(InternalKey(sequenceNumber.incrementAndGet(), key), value)
      case DeleteOperation(key) =>
        InternalDeleteOperation(InternalKey(sequenceNumber.incrementAndGet(), key))
    }
    memTableWriter.writeBatch(WriteBatch(internalOps:_*))
  }

  def delete(key: T): Unit = {
    val internalKey = InternalKey(sequenceNumber.incrementAndGet(), key)
    memTableWriter.delete(internalKey)
  }
}


object KVStore {
  val MaxMemTableSize = 20*1024*1024

  def open[T](dbPath: String)(implicit sliceable: Sliceable[T]): KVStore[T] = {
    val SSTableFactory = new SSTableFactoryImpl[T](dbPath)
    val sstables = SSTableFactory.recoverSSTables

    val ssTableManager = new SSTableManagerImpl[T](dbPath, sstables, SSTableFactory, sliceable)

    val wal = new WriteAheadLogImpl(dbPath, sliceable)
    val iterator = wal.getLogStream

    val memTableFactory = new MemTableFactoryImpl[T](dbPath, sliceable)
    val initMemTable = memTableFactory.fromLogStream(iterator)
    val memTableWriter = new MemTableWriterImpl[T](ssTableManager, sliceable, wal, initMemTable, memTableFactory)

    // we need to store lasy known sequence id somewhere.
    new KVStoreImpl[T](ssTableManager, memTableWriter,
      0L, sliceable)
  }

  def close(kvStore: KVStore[_]): Unit = {
    // close reading
    kvStore.ssTableManager.close()
    // close writing
    kvStore.memTableWriter.close()
  }
}
