package com.treadstone90.mkvstore

import java.io.{DataOutputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.util.zip.CRC32

/**
 * write ahead log. append some log entries.
 * The operation needs to also
 * and sequence number is imporant
 * Don't worry about the effficiency yet.
 * so pack key | sequence_number | type | value into the WAL format.
 * So firtly we'll make sure that we are able to write to the log.
 * Then we can change the log format.
 */

trait WriteAheadLog {
  def appendEntry(logEntry: LogEntry)
}

class WriteAheadLogImpl(dbPath: String) extends WriteAheadLog {
  private val dataOutputStream = new DataOutputStream(new FileOutputStream(s"$dbPath/LOG.current"))

  def appendEntry(logEntry: LogEntry): Unit = {
    // create one ByteBuffer and write once instead of doing this.
    val record = logEntry.serializeToBytes
    dataOutputStream.writeInt(record.size)
    dataOutputStream.writeLong(record.checkSum)
    dataOutputStream.write(record.data)
  }

  def close(): Unit = {
    dataOutputStream.close()
  }
}

case class Record(size: Int, checkSum:Long, data: Array[Byte])

sealed trait LogEntry {
  def keySize: Int
  def key: Array[Byte]
  def sequenceId: Long
  def operationType: OperationType
  def valueSize: Int
  val logEntrySize: Int

  def writeValue(buffer: ByteBuffer): Unit = {}

  def serializeToBytes: Record = {
    val buf = ByteBuffer.allocate(logEntrySize)
    buf.putInt(keySize)
    buf.put(key)
    buf.putLong(sequenceId)
    buf.putInt(operationType.ordinal())
    writeValue(buf)
    val crc = new CRC32()
    crc.update(buf)

    Record(logEntrySize, crc.getValue, buf.array())
  }
}

case class WriteLogEntry(keySize: Int, key: Array[Byte], sequenceId: Long, valueSize: Int, value: Array[Byte])
  extends LogEntry {

  override def writeValue(buffer: ByteBuffer): Unit = {
    buffer.putInt(valueSize)
    buffer.put(value)
  }

  val logEntrySize: Int = {
    // size of internalKey | key + sequenceId | operationType | valueSize | value
    4 + keySize + 8 + 4 +  4 + valueSize
  }

  val operationType: OperationType = OperationType.VALUE_OP
}

case class DeleteLogEntry(keySize: Int, key: Array[Byte], sequenceId: Long) extends LogEntry {
  val valueSize = 0
  val operationType: OperationType = OperationType.DELETE_OP

  val logEntrySize: Int = {
    // size of internalKey | key + sequenceId | operationType | valueSize | value
    4 + keySize + 8 + 4
  }
}

object LogEntry {
  def deserializeFromRecord(record: Record): LogEntry = {
    val byteBuffer = ByteBuffer.wrap(record.data)
    val keySize = byteBuffer.getInt
    val keyByteArray = new Array[Byte](keySize)
    byteBuffer.get(keyByteArray)
    val sequenceId = byteBuffer.getLong
    val operationType = OperationType.fromInteger(byteBuffer.getInt())
    operationType match {
      case OperationType.DELETE_OP => DeleteLogEntry(keySize, keyByteArray, sequenceId)
      case OperationType.VALUE_OP =>
        val valueSize = byteBuffer.getInt
        val valueByteArray = new Array[Byte](valueSize)
        byteBuffer.get(valueByteArray)
        WriteLogEntry(keySize, keyByteArray, sequenceId, valueSize, valueByteArray)
    }
  }
}