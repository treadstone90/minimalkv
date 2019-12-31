package com.treadstone90.mkvstore.raft

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Predicate

import com.google.common.util.concurrent.AbstractIdleService
import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.inject.Logging
import com.twitter.util.{Duration, Future, Promise, Timer}

import scala.collection.concurrent.TrieMap
import scala.collection.convert.{AsJavaConverters, AsScalaConverters}

class Tracker(raftState: RaftState, raftClients: Map[String, RaftClient], timer: Timer,
              wal: WriteAheadLog[RaftLogEntry])
  extends AbstractIdleService with AsScalaConverters with AsJavaConverters with Logging {

  val nextIndices: TrieMap[String, Long] = TrieMap.empty[String, Long].++:(raftClients.keySet.map { peer =>
    peer -> (wal.length + 1)
  }.toSeq)

  val matchIndices: TrieMap[String, Long] = TrieMap.empty[String, Long].++:(raftClients.keySet.map { peer =>
    peer -> 0L
  }.toSeq)

  val inFlightRequests: ConcurrentHashMap[Long, Promise[Unit]] = new ConcurrentHashMap[Long, Promise[Unit]]()

  override def startUp(): Unit = {
    moveCommitIndex()
  }

  override def shutDown(): Unit = {

  }

  def trackRequest(index: Long): Future[Unit] = {
    val promise = Promise[Unit]()
    inFlightRequests.put(index, promise)
    promise
  }

  def moveCommitIndex(): Unit = {
    moveCommitIndexOnce()
    Future.sleep(Duration.fromMilliseconds(5))(timer).before(Future{moveCommitIndex()})
  }

  // or I can allocate one thread for this ? what the hell
  def moveCommitIndexOnce(): Unit = {
    // see if any inflight requests can be completed.
    // How do we know which requests to poll or get.
    val entrySet = nextIndices.toSeq

    val appendEntriesResponses = entrySet.map { case(processId, nextIndex) =>
      val entries = wal.slice(nextIndex, nextIndex + 1024).toSeq
      val logEntryMeta = wal.get(nextIndex - 1).map { entry =>
        RaftLogMetadata(nextIndex - 1, entry.term)
      }
      // check if entries is empty here
      val appendEntriesEvent = AppendEntriesRequest(raftState.currentTerm.get, raftState.processId, logEntryMeta,
        entries, raftState.commitIndex)

      raftClients(processId).appendEntries(appendEntriesEvent)
        .map { response =>
          if(response.success) {
            nextIndices.put(processId, nextIndex + entries.length)
            matchIndices.put(processId, nextIndex + entries.length - 1)
          } else {
            nextIndices.put(processId, nextIndex - 1)
          }
        }.onFailure { t =>
          error(s"Request failed because ${t.getMessage}")
      }
    }

    val responses = Future.collectToTry(appendEntriesResponses)

    val pastCommitIndex = matchIndices.filter { case(_, v) => v > raftState.commitIndex}.values.toSeq

    assert(raftState.currentTerm.nonEmpty)
    // we are only committing in the current term . Why ?
    // here we are moving commit index to smallest. But if we can find a value that is present in "quorum"
    // machines that is good enough.
    if(pastCommitIndex.length + 1 >= raftState.quorum &&  wal.get(pastCommitIndex.min).map(_.term) == raftState.currentTerm) {
      raftState.commitIndex = pastCommitIndex.min
      // how can we complete some inflight requests.
      inFlightRequests.entrySet().removeIf( new Predicate[java.util.Map.Entry[Long, Promise[Unit]]] {
        def test(t: java.util.Map.Entry[Long, Promise[Unit]]): Boolean = {
          if(t.getKey <= raftState.commitIndex) {
            t.getValue.setValue(Unit)
            true
          } else {
            false
          }
        }
      })
    }
  }
}
