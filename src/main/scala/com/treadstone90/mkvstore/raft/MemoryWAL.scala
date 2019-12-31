package com.treadstone90.mkvstore.raft

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Predicate

import com.treadstone90.mkvstore.WriteAheadLog

import scala.collection.convert.{AsJavaConverters, AsScalaConverters}

class MemoryWAL extends WriteAheadLog[RaftLogEntry] with AsScalaConverters with AsJavaConverters {
  val log: ConcurrentSkipListMap[Long, RaftLogEntry] = new ConcurrentSkipListMap[Long, RaftLogEntry]()
  var lastIndex = new AtomicLong(0)

  def appendEntry(logEntry: RaftLogEntry): Long = {
    val nextIndex = lastIndex.incrementAndGet()
    log.put(nextIndex, logEntry)
    nextIndex
  }

  def getLogStream: Iterator[RaftLogEntry] = asScalaIterator(log.values().iterator())

  def rolloverFile(): Unit = {}

  def appendEntries(logEntry: Seq[RaftLogEntry]): Unit = {}

  def get(index: Long): Option[RaftLogEntry] = {
    Option(log.get(index))
  }

  // includes startIndex excludes endIndex
  def slice(startIndex: Long, endIndex: Long): Iterator[RaftLogEntry] = {
    asScalaIterator(log.subMap(startIndex,
      Math.min(endIndex, lastIndex.get() + 1)).values().iterator())
  }

  def length: Long = log.size()

  // keep all entries in the log unptil index.
  // anything more than index is truncated.
  def truncate(index: Long): Unit = {
    log.entrySet().removeIf(new Predicate[java.util.Map.Entry[Long, RaftLogEntry]] {
      def test(t: java.util.Map.Entry[Long, RaftLogEntry]): Boolean = {
        t.getKey > index
      }
    })
  }
}

trait RaftLogEntry {
  def term: Long
}

case class SimpleRaftLogEntry(command: String, term: Long) extends RaftLogEntry
