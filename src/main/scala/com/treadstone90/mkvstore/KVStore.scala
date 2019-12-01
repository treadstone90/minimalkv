package com.treadstone90.mkvstore

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicLong

import com.google.common.hash.{Funnel, PrimitiveSink}

import scala.collection.convert.AsScalaConverters
import scala.collection.mutable


/**
 * Stop the KV Store with write ahead log and compaction.
 * Like enough of this. Then I want you to focus on
 * replication and sharding. and consensus.
 * @tparam K
 */
trait KVStore[K] {
  /**
   * Read a Needle from a KVStore.
   * Needle is an encapsulation of a result.
   * @param key Key is a byte array
   * @return Option[Needle]
   */
  def get(key: K): Option[Needle]

  def put(key: K, value: Needle): Unit

  def delete(key: K): Unit

  /**
   *
//   * @param needle
   */
//  def write(needle: Needle): Unit
}

case class InternalKey[T](sequenceId: Long, key: T)

trait Sliceable[T] {
  def keyOrdering: Ordering[InternalKey[T]] = {
    (x: InternalKey[T], y: InternalKey[T]) => {
      if (ordering.equiv(x.key, y.key)) {
        Ordering.Long.compare(y.sequenceId, x.sequenceId)
      } else {
        ordering.compare(x.key, y.key)
      }
    }
  }
  def asByteBuffer: T => ByteBuffer
  def ordering: Ordering[T]
  def fromByteBuffer: ByteBuffer => T
  def funnel: Funnel[T]
}

object Sliceable {
  implicit val long2Slice :Sliceable[Long] = {
    new Sliceable[Long] {
      def asByteBuffer: Long => ByteBuffer = long => ByteBuffer.allocate(java.lang.Long.BYTES).putLong(long)
      val ordering: Ordering[Long] = Ordering.Long
      def fromByteBuffer: ByteBuffer => Long = buf => buf.getLong
      val funnel = (from: Long, into: PrimitiveSink) => into.putLong(from)
    }
  }

  implicit val str2Slice: Sliceable[String] = {
    new Sliceable[String] {
      def asByteBuffer: String => ByteBuffer = {
        str =>
          val bytes = str.getBytes
          ByteBuffer.allocate(bytes.length).put(bytes)
      }
      val ordering: Ordering[String] = Ordering.String
      def fromByteBuffer: ByteBuffer => String = {
        buf => new String(buf.array())
      }
      val funnel = (from: String, into: PrimitiveSink) => into.putString(from, Charset.defaultCharset())
    }
  }

  implicit val int2Slice :Sliceable[Int] = {
    new Sliceable[Int] {
      def asByteBuffer: Int => ByteBuffer = {
        (x: Int) => ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(x)
      }
      val ordering: Ordering[Int] = Ordering.Int
      def fromByteBuffer: ByteBuffer => Int = buf => buf.getInt

      val funnel = (from: Int, into: PrimitiveSink) => {
        into.putInt(from)
      }
    }
  }

}

// so we need to take a key type to a byte array
// then we are good.
// The treemaps needs an ordering of this byte array. hmm.
class KVStoreImpl[T](ssTableManager: SSTableManager[T],
                     writeAheadLog: WriteAheadLog,
                     memTableWriter: MemTableWriter[T],
                     sliceable: Sliceable[T]) extends KVStore[T] with AsScalaConverters {

  // Slice
  // The map should operator on a type which knows about the ByteBuffer.
  var memTable = new mutable.TreeMap[T, Array[Byte]]()(sliceable.ordering)

  var currentMemTableSize = 0
  val sequenceNumber = new AtomicLong(0)

  /**
   * Read a Needle from a KVStore.
   * Needle is an encapsulation of a result.
   *
   * @param key Key is a byte array
   * @return Option[Needle]
   */
  def get(key: T): Option[Needle] = {
    memTableWriter.get(InternalKey(Long.MaxValue, key)).orElse(ssTableManager.get(key)).flatMap { logEntry =>
      logEntry match {
        case WriteLogEntry(_, _, _, valueSize, value) => Some(Needle(valueSize, value))
        case _ => None
      }
    }
  }

  def put(key: T, value: Needle): Unit = {
    // we need to first write to write ahead log.
    // and then persist to the
    val internalKey = InternalKey(sequenceNumber.incrementAndGet(), key)
    memTableWriter.put(internalKey, value)
  }

  def delete(key: T): Unit = {
    val internalKey = InternalKey(sequenceNumber.incrementAndGet(), key)
    memTableWriter.delete(internalKey)
  }
}


object KVStore {
  val MaxMemTableSize = 20*1024*1024

  def apply[T](dbPath: String)(implicit sliceable: Sliceable[T]): KVStore[T] = {
    // 1. we need to load the ssTableManage
    // 2. what about in memory ?
    // We actually need to have the WAL for that.
    val SSTableFactory = new SSTableFactoryImpl[T](dbPath, sliceable)
    val ssTableManager = new SSTableManagerImpl[T](dbPath, SSTableFactory, sliceable)
    ssTableManager.bootStrapSSTables(dbPath)
    val wal = new WriteAheadLogImpl(dbPath)
    val memTableFactory = new MemTableFactoryImpl[T](dbPath, sliceable)
    val memTableWriter = new MemTableWriterImpl[T](ssTableManager, sliceable, memTableFactory)
    new KVStoreImpl[T](ssTableManager, wal, memTableWriter, sliceable)
  }
}