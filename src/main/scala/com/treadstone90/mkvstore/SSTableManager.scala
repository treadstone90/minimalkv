package com.treadstone90.mkvstore

import java.nio.file.{Files, Paths}

import com.twitter.util.{Future, FuturePool}

import scala.collection.convert.AsScalaConverters
import scala.collection.mutable

case class SSTableEntryMeta(size: Int, offset: Long, checkSum: Long)

/**
 * Initialize DB from file.
 *
 */

trait SSTableManager[T] {
  def sliceable: Sliceable[T]
  def get(key: T): Option[LogEntry]
  def writeMemTable(memTable: MemTable[T]): SSTable[T]
  def bootStrapSSTables(dbPath: String): Unit
}

class SSTableManagerImpl[T](dbPath: String,
                            ssTableFactory: SSTableFactory[T],
                            val sliceable: Sliceable[T]) extends SSTableManager[T] with AsScalaConverters {

  val ssTableMap = new mutable.LinkedHashMap[Int, SSTable[T]]()
  var CurrentSSTableSegment = 0

  def get(key: T): Option[LogEntry] = {
    println(s"Going to SSTable for $key")
    var needle: Option[LogEntry] = None
    val valuesIterator = ssTableMap.valuesIterator
    while(valuesIterator.hasNext && needle.isEmpty) {
      needle = valuesIterator.next().get(key)
    }
    needle
  }

  def writeMemTable(memTable: MemTable[T]): SSTable[T] = {
    val result = ssTableFactory.fromMemTable(memTable, CurrentSSTableSegment)
    ssTableMap.put(CurrentSSTableSegment, result)
    CurrentSSTableSegment += 1
    result
  }

  def bootStrapSSTables(dbPath: String): Unit = {
    val files = asScalaIterator(Files.list(Paths.get(dbPath)).iterator())
    files.withFilter(file => file.endsWith(".sst") && file.startsWith("part")).foreach { file =>
      val Array(_, segmentId) = file.getFileName.toString.split("-")
      val ssTable = ssTableFactory.fromFile(segmentId.toInt)
      ssTableMap.put(segmentId.trim.toInt, ssTable)
    }
  }
}

object SSTableManager {
  val KeyInterval: Long = 10L*1024
}