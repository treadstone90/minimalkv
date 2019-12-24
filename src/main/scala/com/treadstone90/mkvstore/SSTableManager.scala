package com.treadstone90.mkvstore

import java.io.Closeable

import com.twitter.util.{Future, FuturePool}

import scala.collection.convert.AsScalaConverters

case class RecordEntryMeta(size: Int, offset: Long, checkSum: Long)

/**
 * Initialize DB from file.
 *
 */

trait SSTableManager[T] extends Closeable {
  def sliceable: Sliceable[T]
  def get(key: T): Option[LogEntry]
  def writeMemTable(memTable: MemTable[T]): Future[SSTable[T]]
}

class SSTableManagerImpl[T](dbPath: String,
                            initialSSTables: Seq[(Int, SSTable[T])],
                            ssTableFactory: SSTableFactory[T],
                            val sliceable: Sliceable[T]) extends SSTableManager[T] with AsScalaConverters {

  private var ssTables = initialSSTables
  private var CurrentSSTableSegment = if(initialSSTables.isEmpty) {
    0
  } else {
    initialSSTables.maxBy(_._1)._1 + 1
  }

  println(s"Next SSTable write is $CurrentSSTableSegment")

  def get(key: T): Option[LogEntry] = {
    println(s"Going to SSTable for $key")
    var needle: Option[LogEntry] = None
    val valuesIterator = ssTables.iterator
    while(valuesIterator.hasNext && needle.isEmpty) {
      val (k, v) = valuesIterator.next()
      println(s"SST id $k")
      needle = v.get(key)
    }
    needle
  }

  def writeMemTable(memTable: MemTable[T]): Future[SSTable[T]] = {
    FuturePool.unboundedPool {
      val result = ssTableFactory.fromMemTable(memTable, CurrentSSTableSegment)
      ssTables = ssTables.+:((CurrentSSTableSegment, result))
      CurrentSSTableSegment += 1
      result
    }
  }

  def close(): Unit = {
    ssTables.foreach(_._2.close())
  }
}

object SSTableManager extends AsScalaConverters {
  val KeyInterval: Long = 512L*1024
}