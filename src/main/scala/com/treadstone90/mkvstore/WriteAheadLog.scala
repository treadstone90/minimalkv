package com.treadstone90.mkvstore

import java.io.{Closeable, RandomAccessFile}
import java.nio.file.{FileSystems, Files, StandardCopyOption}

/**
 * write ahead log. append some log entries.
 * The operation needs to also
 * and sequence number is imporant
 * Don't worry about the effficiency yet.
 * so pack key | sequence_number | type | value into the WAL format.
 * So firstly we'll make sure that we are able to write to the log.
 * Then we can change the log format.
 */

trait WriteAheadLog {
  def appendEntry(logEntry: LogEntry)
  def getLogStream: LogEntryIterator
  def rolloverFile(): Unit
  def appendEntries(logEntry: Seq[LogEntry])
}

class WriteAheadLogImpl[T](dbPath: String,
                           val sliceable: Sliceable[T])
  extends WriteAheadLog with LogEntryUtils with Closeable {
  val currentLogFileName = s"$dbPath/LOG.current"
  val rolledLogFileName = s"$dbPath/LOG.old"
  val newLogFileName = s"$dbPath/LOG.new"
  private var dataOutputStream = new RandomAccessFile(currentLogFileName, "rwd")
  dataOutputStream.seek(dataOutputStream.length())

  def appendEntry(logEntry: LogEntry): Unit = {
    val record = logEntry.makeRecord
    dataOutputStream.write(serializeRecord(record))
  }

  def appendEntries(logEntries: Seq[LogEntry]): Unit = {
    val bytes = serializeRecords(logEntries.map(_.makeRecord))
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

  def getLogStream: LogEntryIterator = {
    new LogEntryIterator(dbPath)
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
