package com.treadstone90.mkvstore

import java.io.{ByteArrayOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentSkipListMap

import com.google.common.hash.BloomFilter
import com.twitter.util.{Await, Future, FuturePool}

import scala.collection.AbstractIterator
import scala.collection.convert.AsScalaConverters

trait SSTableFactory[T] {
  def fromMemTable(memTable: MemTable[T], segmentId: Int): SSTable[T]
  def fromFile(segmentId: Int): SSTable[T]
  def recoverSSTables: Seq[(Int, SSTable[T])]
  def mergeSSTables(sst1: SSTable[T], sst2: SSTable[T], segmentId: Int): SSTable[T]
}

/***
 * This class is thread safe and there is no reason for it to not be accessble from multiple threads.
 * At the same we would ideally never need multiple threads here.
 * @param sliceable
 * @tparam T
 */
class SSTableFactoryImpl[T](dbPath: String)(implicit val sliceable: Sliceable[T])
  extends SSTableFactory[T]
    with SstUtils[T]
    with LogEntryUtils
    with AsScalaConverters {

  val entrySerializer: LogEntry => Record = LogEntry.serializeLogEntry

  def writeHeader(bloomFilter: BloomFilter[T], segmentId: Int,
                  writeRandomAccessFile: RandomAccessFile): Int = {

    val bfByteArrayOp = new ByteArrayOutputStream()
    bloomFilter.writeTo(bfByteArrayOp)

    val headerSize = SSTable.headerSize(bfByteArrayOp.size())
    // allocate 4 more bytes for the size of the header.
    val headerByteBuffer = ByteBuffer.allocate(headerSize + 4)

    // THis is the RecordIO for the header.
    // header size + Header.
    // Writes out the headerSize to buf
    headerByteBuffer.putInt(headerSize)

    // writes out the header to buf
    headerByteBuffer.putInt(segmentId)
    headerByteBuffer.putInt(bfByteArrayOp.size())
    headerByteBuffer.put(bfByteArrayOp.toByteArray)

    // writes out headerBuf to file.
    writeRandomAccessFile.write(headerByteBuffer.array())
    headerSize
  }

  private def fromEntries(iterator: Iterator[(InternalKey[T], LogEntry)],
                          bloomFilter: BloomFilter[T], segmentId: Int) = {

    val ssTableIndexMap = new ConcurrentSkipListMap[T, RecordEntryMeta](sliceable.ordering)

    val ssTableFile = getSSTFile(segmentId)
    val writeRandomAccessFile = new RandomAccessFile(ssTableFile, "rwd")

    val headerSize = writeHeader(bloomFilter, segmentId, writeRandomAccessFile)
    // TODO this can be part of the footer. and if it is a fixed length footer we can

    var currentWritten = 0L

    var prevKey: Option[T] = None

    while(iterator.hasNext) {
      val (k, value) = iterator.next()

      if(!prevKey.contains(k.key)) {
        println(s"writing ${k.key} to $ssTableFile")
        val record = entrySerializer(value)
        val metadata = RecordEntryMeta(record.size, writeRandomAccessFile.getFilePointer, record.checkSum)
        writeRandomAccessFile.write(serializeRecord(record))

        if (currentWritten == 0 || currentWritten > SSTableManager.KeyInterval) {
          ssTableIndexMap.put(k.key, metadata)
          currentWritten = 0L
        }
        currentWritten += metadata.size
      }
      prevKey = Some(k.key)
    }

    println("Writing summary file")
    writeSummaryFile(ssTableIndexMap, segmentId)
    writeRandomAccessFile.close()

    val readRandomAccessFile = new RandomAccessFile(ssTableFile, "r")
    readRandomAccessFile.seek(headerSize)

    new SSTableImpl[T](ssTableIndexMap, segmentId, bloomFilter, sliceable, ssTableFile, readRandomAccessFile)
  }

  def fromMemTable(memTable: MemTable[T], segmentId: Int): SSTable[T] = {
    val bloomFilter = BloomFilter.create[T](sliceable.funnel, memTable.size)
    memTable.keys.foreach(buf => bloomFilter.put(buf.key))
    fromEntries(memTable.entries, bloomFilter, segmentId)
  }


  private def getSSTFile(segmentId: Int): String = {
    s"$dbPath/part-$segmentId.sst"
  }

  private def getSummaryFile(segmentId: Int): String = {
    s"$dbPath/part-$segmentId.summary"
  }


  def writeSummaryFile(concurrentSkipListMap: ConcurrentSkipListMap[T, RecordEntryMeta],
                       segmentId: Int): Unit = {
    val summaryFile = new RandomAccessFile(getSummaryFile(segmentId), "rwd")

    mapAsScalaConcurrentMap(concurrentSkipListMap).foreach { case(key, value) =>
      writeSummaryRecord(key, value, summaryFile)
    }
    summaryFile.close()
  }

  def writeSummaryRecord(key: T, meta: RecordEntryMeta,
                         writeRandomAccessFile: RandomAccessFile): Unit = {
    val byteBuffer = sliceable.asByteBuffer(key)
    writeRandomAccessFile.writeInt(byteBuffer.array().length)
    writeRandomAccessFile.write(byteBuffer.array())
    writeRandomAccessFile.writeInt(meta.size)
    writeRandomAccessFile.writeLong(meta.offset)
    writeRandomAccessFile.writeLong(meta.checkSum)
  }

  def readSummaryFile(filePath: String): ConcurrentSkipListMap[T, RecordEntryMeta] = {
    val readRandomAccessFile = new RandomAccessFile(filePath, "r")
    val map = new ConcurrentSkipListMap[T, RecordEntryMeta](sliceable.ordering)
    val bytes = new Array[Byte](readRandomAccessFile.length().toInt)
    readRandomAccessFile.readFully(bytes)
    val byteBuffer = ByteBuffer.wrap(bytes)

    while(byteBuffer.hasRemaining) {
      val keySize = byteBuffer.getInt
      val keyBytes = new Array[Byte](keySize)
      byteBuffer.get(keyBytes)

      val size = byteBuffer.getInt()
      val offset = byteBuffer.getLong
      val checkSum = byteBuffer.getLong
      map.put(sliceable.fromByteArray(keyBytes),
        RecordEntryMeta(size, offset, checkSum))
    }
    readRandomAccessFile.close()
    map
  }

  def fromFile(segmentId: Int): SSTable[T] = {
    val sstFile = getSSTFile(segmentId)
    val randomAccessFile = new RandomAccessFile(sstFile, "r")
    val metadataMap = readSummaryFile(getSummaryFile(segmentId))
    val header = readHeader(randomAccessFile)
    new SSTableImpl[T](metadataMap, header.segmentId, header.bloomFilter, sliceable, sstFile, randomAccessFile)
  }

  def recoverSSTables: Seq[(Int, SSTable[T])] = {
    val files = asScalaIterator(Files.list(Paths.get(dbPath)).iterator()).map(_.toFile)
      .toIndexedSeq.sortBy(_.lastModified())

    val futures = files.withFilter(file => file.getName.endsWith("sst") && file.getName.startsWith("part")).map { file =>
      FuturePool.unboundedPool {
        val Array(_, suffix) = file.getName.split("-")
        val Array(segmentId, _) = suffix.split("""\.""")
        println(s"Bootstrapping $segmentId ")
        (file, segmentId.toInt, fromFile(segmentId.toInt))
      }
    }

    val waitableFuture = Future.collect(futures)
    val ssTables = Await.result(waitableFuture)
    ssTables.sortBy(triple => -triple._1.lastModified()).map(triple => (triple._2, triple._3))
  }

  def mergeSSTables(sst1: SSTable[T], sst2: SSTable[T], segmentId: Int): SSTable[T] = {
    val iterator1 = sst1.iterator
    val iterator2 = sst2.iterator

    val newBf = BloomFilter.create[T](sliceable.funnel, sst1.bloomFilter.approximateElementCount() +
      sst2.bloomFilter.approximateElementCount())

    val entriesIterator = new AbstractIterator[(InternalKey[T], LogEntry)] {
      var sst1Ptr: Option[LogEntry] = None
      var sst2Ptr: Option[LogEntry] = None

      private def advanceSst1() = {
        sst1Ptr = if(iterator1.hasNext) {
          Some(iterator1.next())
        } else {
          None
        }
      }

      private def advanceSst2() = {
        sst2Ptr = if(iterator2.hasNext) {
          Some(iterator2.next())
        } else {
          None
        }
      }

      advanceSst1()
      advanceSst2()

      def hasNext: Boolean = {
        sst1Ptr.nonEmpty || sst2Ptr.nonEmpty
      }

      def next(): (InternalKey[T], LogEntry) = {
        if(sst1Ptr.isEmpty) {
          val entry = sst2Ptr.get
          advanceSst2()
          (InternalKey[T](entry.sequenceId, sliceable.fromByteArray(entry.key)), entry)
        } else if(sst2Ptr.isEmpty) {
          val entry = sst1Ptr.get
          advanceSst1()
          (InternalKey[T](entry.sequenceId, sliceable.fromByteArray(entry.key)), entry)
        } else {
          val (key1, entry1) = (InternalKey[T](sst1Ptr.get.sequenceId, sliceable.fromByteArray(sst1Ptr.get.key)),
            sst1Ptr.get)
          val (key2, entry2) = (InternalKey[T](sst2Ptr.get.sequenceId, sliceable.fromByteArray(sst2Ptr.get.key)),
            sst2Ptr.get)

          if(sliceable.ordering.equiv(key1.key, key2.key)) {
            val result = if(sliceable.keyOrdering.lt(key1, key2)) {
              newBf.put(key1.key)
              (key1, entry1)
            } else {
              newBf.put(key2.key)
              (key2, entry2)
            }
            advanceSst1()
            advanceSst2()
            result
          } else {
            if(sliceable.keyOrdering.lt(key1, key2)) {
              newBf.put(key1.key)
              advanceSst1()
              (key1, entry1)
            } else {
              newBf.put(key2.key)
              advanceSst2()
              (key2, entry2)
            }
          }
        }
      }
    }
    fromEntries(entriesIterator, newBf, segmentId)
   }
}

