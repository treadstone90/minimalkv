package com.treadstone90.mkvstore

import java.io.{Closeable, RandomAccessFile}
import java.nio.file.{FileSystems, Files, StandardCopyOption}
import java.util.concurrent.atomic.AtomicLong

/**
 * write ahead log. append some log entries.
 * The operation needs to also
 * and sequence number is imporant
 * Don't worry about the effficiency yet.
 * so pack key | sequence_number | type | value into the WAL format.
 * So firstly we'll make sure that we are able to write to the log.
 * Then we can change the log format.
 */

trait WriteAheadLog[L] {
  def appendEntry(logEntry: L): Long
  def getLogStream: Iterator[L]
  def rolloverFile(): Unit
  def appendEntries(logEntry: Seq[L])
  // This is the last sequenceId available in the log.
  def slice(startIndex: Long, endIndex: Long): Iterator[L]
  def get(index: Long): Option[L]
  def truncate(index: Long): Unit
  def length: Long
}

trait PersistentWriteAheadLog[L] extends WriteAheadLog[L] {
  def serializer: L => Record
  def deserializer: Record => L
}

class WriteAheadLogImpl[T](dbPath: String,
                           val sliceable: Sliceable[T])
  extends PersistentWriteAheadLog[LogEntry] with LogEntryUtils with Closeable {
  val currentLogFileName = s"$dbPath/LOG.current"
  val rolledLogFileName = s"$dbPath/LOG.old"
  val newLogFileName = s"$dbPath/LOG.new"
  private var dataOutputStream = new RandomAccessFile(currentLogFileName, "rwd")
  dataOutputStream.seek(dataOutputStream.length())
  var index = new AtomicLong(0)
  val serializer: LogEntry => Record = LogEntry.serializeLogEntry
  val deserializer: Record => LogEntry = LogEntry.deserializeFromRecord

  def appendEntry(logEntry: LogEntry): Long = {
    val record = serializer(logEntry)
    dataOutputStream.write(serializeRecord(record))
    index.incrementAndGet()
  }

  def appendEntries(logEntries: Seq[LogEntry]): Unit = {
    val bytes = serializeRecords(logEntries.map(serializer))
    dataOutputStream.write(bytes)
  }

  def close(): Unit = {
    dataOutputStream.close()
  }

  def rolloverFile(): Unit = {
    val oldWAL = dataOutputStream
    Files.createFile(FileSystems.getDefault.getPath(newLogFileName))
    dataOutputStream = new RandomAccessFile(newLogFileName, "rwd")
    oldWAL.close()


    // This can happen async
    Files.move(FileSystems.getDefault.getPath(currentLogFileName),
      FileSystems.getDefault.getPath(rolledLogFileName), StandardCopyOption.ATOMIC_MOVE)

    Files.move(FileSystems.getDefault.getPath(newLogFileName),
      FileSystems.getDefault.getPath(currentLogFileName), StandardCopyOption.ATOMIC_MOVE)


    Files.delete(FileSystems.getDefault.getPath(rolledLogFileName))
  }

  def getLogStream: Iterator[LogEntry] = {
    new LogEntryIterator(dbPath)
  }

  def slice(startIndex: Long, endIndex: Long): Iterator[LogEntry] = new WALRangeIterator(dbPath, startIndex, endIndex)

  def get(index: Long): Option[LogEntry] = Option(new WALRangeIterator(dbPath, index, index).next())

  def length: Long = dataOutputStream.length()

  def truncate(index: Long): Unit = ???
}

class WALRangeIterator(dbPath: String, startIndex: Long, endIndex: Long) extends Iterator[LogEntry] with LogEntryUtils {
  private val dis = new RandomAccessFile(s"$dbPath/LOG.current" , "r")
  var lastKnownSequenceId: Option[Long] = None
  dis.seek(startIndex)

  def hasNext: Boolean = {
    val filePointer = dis.getFilePointer
    filePointer < endIndex && filePointer < dis.length()
  }

  def next(): LogEntry = {
    val record = readRecord(dis)
    val logEntry = LogEntry.deserializeFromRecord(record)
    lastKnownSequenceId = Some(logEntry.sequenceId)
    logEntry
  }
}

class LogEntryIterator(dbPath: String) extends Iterator[LogEntry] with LogEntryUtils {
  private val dis = new RandomAccessFile(s"$dbPath/LOG.current" , "r")
  var lastKnownSequenceId: Option[Long] = None

  def hasNext: Boolean = {
    dis.getFilePointer < dis.length()
  }

  def next(): LogEntry = {
    val record = readRecord(dis)
    val logEntry = LogEntry.deserializeFromRecord(record)
    lastKnownSequenceId = Some(logEntry.sequenceId)
    logEntry
  }
}
