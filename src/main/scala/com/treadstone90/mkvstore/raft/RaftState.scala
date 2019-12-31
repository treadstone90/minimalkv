package com.treadstone90.mkvstore.raft

import java.io.{Closeable, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.CRC32

import com.google.common.util.concurrent.AbstractIdleService
import com.google.inject.Inject
import com.treadstone90.mkvstore.{LogEntryUtils, Record}

// it is ebntirely possible for an follower to not have voted in a term but stil be part of the clusrter.
trait RaftState extends AbstractIdleService with Closeable {
  var commitIndex: Long
  def replicaCount: Int
  def processId: String
  def quorum: Int
  def monotonicUpdate(term: Long, candidate: Option[String]): (Boolean, Option[(Long, Option[String])])
  def incrementTerm(): Long
  def currentTerm: Option[Long]
  def currentCandidate: Option[String]
}

case class VoteEntry(term: Long, votedCandidate: Option[String]) {
  def toRecord: Record = {
    val candidateLength = votedCandidate.map(_.length).getOrElse(0)
    val buf = ByteBuffer.allocate(java.lang.Long.BYTES + java.lang.Integer.BYTES + 128)
    buf.putLong(term)
    buf.putInt(candidateLength)

    if(votedCandidate.nonEmpty) {
      buf.put(votedCandidate.get.getBytes)
    }

    val crc = new CRC32()
    val byteArray = buf.array()
    crc.update(byteArray)
    Record(java.lang.Long.BYTES + java.lang.Integer.BYTES + 128, crc.getValue, byteArray)
  }
}

object VoteEntry {
  def fromRecord(record: Record): VoteEntry = {
    val dataBuffer = ByteBuffer.wrap(record.data)
    val term = dataBuffer.getLong()
    val candidateSize = dataBuffer.getInt
    val candidateId = if(candidateSize > 0) {
      val byteArray = new Array[Byte](candidateSize)
      dataBuffer.get(byteArray)
      Some(new String(byteArray.take(candidateSize)))
    } else {
      None
    }
    VoteEntry(term, candidateId)
  }
}


class RaftStateImpl @Inject() (val replicaCount: Int,
                               val processId: String,
                               filePath: String)  extends RaftState with LogEntryUtils {

  val randomAccessFile = new RandomAccessFile(filePath, "rwd")
  val _currentVotingTerm = new AtomicReference[Option[(Long, Option[String])]]()
  val quorum = Math.floorDiv(replicaCount, 2) + 1


  // update the term.
  // return false if vote was a failure also we need a "reason" why it failed. And the reason is the term and
  // return (true, None) is vote was a success
  // return (false, Some(term, candidate) if vote failed.

  def startUp(): Unit = {
    if(randomAccessFile.length() >= 140) {
      randomAccessFile.seek(randomAccessFile.length() - (140 + 12))
      val record = readRecord(randomAccessFile)
      val voteEntry = VoteEntry.fromRecord(record)
      _currentVotingTerm.set(Some(voteEntry.term, voteEntry.votedCandidate))
    } else {
      _currentVotingTerm.set(None)
    }
  }


  def monotonicUpdate(term: Long, candidate: Option[String]): (Boolean, Option[(Long, Option[String])]) = {
    var current = _currentVotingTerm.get()
    var next: Option[(Long, Option[String])] = current
    var isVoteGranted = false
      do {
        isVoteGranted = false
        current = _currentVotingTerm.get()
        next = current

        if(current.isEmpty) {
          next = Some((term, candidate))
          isVoteGranted = true
        } else if(term > current.get._1) {
          next = Some((term, candidate))
          isVoteGranted = true
        } else if(term < current.get._1) {
          return (false, current)
        } else if(candidate == current.get._2 || current.get._2.isEmpty) {
          return (true, current)
        } else if (candidate.isEmpty) {
          return (true, current)
        } else {
          return (false, current)
        }
      } while (!_currentVotingTerm.compareAndSet(current, next))

    // so whoever is able to set can enter this block here
    // how can you ensure that the person with greatest record wins.
    if(_currentVotingTerm.get() == next) {
      val record = VoteEntry(term, candidate).toRecord
      // all threads are waiting here looking to write the state.

      randomAccessFile.write(serializeRecord(record))
    }
    (isVoteGranted, next)
  }

  // will automatically set vote to current process.
  def incrementTerm(): Long = {
    var current = _currentVotingTerm.get()
    var next: Option[(Long, Option[String])] = current

    do {
      current = _currentVotingTerm.get()
      if(current.isEmpty) {
        next = Some((0, Some(processId)))
      } else {
        next = Some((current.get._1 + 1, Some(processId)))
      }
    } while (!_currentVotingTerm.compareAndSet(current, next))

    if(_currentVotingTerm.get() == next) {
      val record = VoteEntry(next.get._1, next.get._2).toRecord
      randomAccessFile.write(serializeRecord(record))
    }
    next.get._1
  }

  def currentTerm: Option[Long] = _currentVotingTerm.get().map(_._1)

  def currentCandidate: Option[String] = _currentVotingTerm.get().flatMap(_._2)

  def close(): Unit = {
    randomAccessFile.close()
  }

  override def shutDown(): Unit = {
    close()
  }

  var commitIndex: Long = 0
}
