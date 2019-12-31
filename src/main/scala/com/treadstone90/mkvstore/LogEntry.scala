package com.treadstone90.mkvstore

import java.io.DataInput
import java.nio.ByteBuffer
import java.util.zip.CRC32

trait LogEntryUtils {
  def readRecord(dataInput: DataInput): Record = {
    val recordSize = dataInput.readInt()
    println(recordSize)
    val checkSum = dataInput.readLong()
    val dataByteArray = new Array[Byte](recordSize)
    dataInput.readFully(dataByteArray)
    Record(recordSize, checkSum, dataByteArray)
  }

  def serializeRecord(record: Record): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(record.size + 4 + 8)
    byteBuffer.putInt(record.size)
    byteBuffer.putLong(record.checkSum)
    byteBuffer.put(record.data)
    byteBuffer.array
  }

  def serializeRecords(record: Seq[Record]): Array[Byte] = {
    val sizeOfRecords = record.foldLeft(0) { case(prev, record) =>
      prev + (record.size + 4 + 8)
    }
    val byteBuffer = ByteBuffer.allocate(sizeOfRecords)
    record.foreach { record =>
      byteBuffer.putInt(record.size)
      byteBuffer.putLong(record.checkSum)
      byteBuffer.put(record.data)
    }
    byteBuffer.array()
  }
}

case class Record(size: Int, checkSum: Long, data: Array[Byte])

trait LogEntry {
  def keySize: Int
  def key: Array[Byte]
  def sequenceId: Long
  def operationType: OperationType
  def valueSize: Int
  val logEntrySize: Int

  def writeValue(buffer: ByteBuffer): Unit = {}
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

  def serializeLogEntry(logEntry: LogEntry): Record = {
    val buf = ByteBuffer.allocate(logEntry.logEntrySize)
    buf.putInt(logEntry.keySize)
    buf.put(logEntry.key)
    buf.putLong(logEntry.sequenceId)
    buf.putInt(logEntry.operationType.ordinal())
    logEntry.writeValue(buf)

    val crc = new CRC32()
    val byteArray = buf.array()
    crc.update(byteArray)

    Record(logEntry.logEntrySize, crc.getValue, byteArray)
  }
}
