package com.treadstone90.mkvstore.raft

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger

import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.util.{Await, Duration, Future, MockTimer, Throw, TimeoutException}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._


class TrackerTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach {

  var raftStateImpl: RaftState = _
  var tempFile = File.createTempFile("raftmeta", "Trackertest").getAbsolutePath
  var raftClients: Map[String, RaftClient] = Map.empty
  var wal: WriteAheadLog[RaftLogEntry] = _
  var tracker: Tracker = _

  override def beforeEach(): Unit = {
    raftStateImpl = new RaftStateImpl(3, "process-0", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.incrementTerm()
    wal = new MemoryWAL
    raftClients = (1 to 2).map { number =>
      s"process-$number" -> mock[RaftClient]
    }.toMap
    tracker = new Tracker(raftStateImpl, raftClients, new MockTimer, wal)
  }

  override def afterEach(): Unit = {
    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    Files.delete(Paths.get(tempFile))
  }

  "Tracker" should "correctly add to an empty when there are no follower failures." in {
    val entries = Seq(SimpleRaftLogEntry("hello1", 0), SimpleRaftLogEntry("hello2", 0),
      SimpleRaftLogEntry("hello3", 0))
    val sucessCount = new AtomicInteger(0)

    wal.appendEntry(SimpleRaftLogEntry("hello1", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello2", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello3", 0))

    val f1 = tracker.trackRequest(0).onSuccess(_ => sucessCount.incrementAndGet())
    val f2 = tracker.trackRequest(1).onSuccess(_ => sucessCount.incrementAndGet())
    val f3 = tracker.trackRequest(2).onSuccess(_ => sucessCount.incrementAndGet())

    val request = AppendEntriesRequest(0, "process-0",
      None, entries, 0)

    val successResponse1 = AppendEntriesResponse(0, true, "process-1")
    val successResponse2 = AppendEntriesResponse(0, true, "process-2")

    when(raftClients("process-1").appendEntries(request)).thenReturn(Future(successResponse1))
    when(raftClients("process-2").appendEntries(request)).thenReturn(Future(successResponse2))
    tracker.moveCommitIndexOnce()
    raftStateImpl.commitIndex should be (3)
    Await.result(Future.join(f1, f2, f3))
    sucessCount.get() should be(3)
    tracker.inFlightRequests.isEmpty shouldBe(true)
  }

  "Tracker" should "correctly add to an empty when there are less than quorum failures" in {
    val entries = Seq(SimpleRaftLogEntry("hello1", 0), SimpleRaftLogEntry("hello2", 0),
      SimpleRaftLogEntry("hello3", 0))
    val sucessCount = new AtomicInteger(0)

    wal.appendEntry(SimpleRaftLogEntry("hello1", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello2", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello3", 0))

    val f1 = tracker.trackRequest(0).onSuccess(_ => sucessCount.incrementAndGet())
    val f2 = tracker.trackRequest(1).onSuccess(_ => sucessCount.incrementAndGet())
    val f3 = tracker.trackRequest(2).onSuccess(_ => sucessCount.incrementAndGet())

    val request = AppendEntriesRequest(0, "process-0",
      None, entries, 0)

    val successResponse1 = AppendEntriesResponse(0, true, "process-1")
    val failuresResponse2 = AppendEntriesResponse(0, false, "process-2")

    when(raftClients("process-1").appendEntries(request)).thenReturn(Future(successResponse1))
    when(raftClients("process-2").appendEntries(request)).thenReturn(Future(failuresResponse2))
    tracker.moveCommitIndexOnce()
    tracker.nextIndices should be (Map(
      "process-1" -> 4L,
      "process-2" -> 0L,
    ))
    tracker.matchIndices should be (Map(
      "process-1" -> 3L,
      "process-2" -> 0L,
    ))
    raftStateImpl.commitIndex should be (3)
    Await.result(Future.join(f1, f2, f3), Duration.fromMilliseconds(0))
    sucessCount.get() should be(3)
  }

  "Tracker" should "not finish future when replication fails due log inconsistency" in {
    val entries = Seq(SimpleRaftLogEntry("hello1", 0), SimpleRaftLogEntry("hello2", 0),
      SimpleRaftLogEntry("hello3", 0))
    val sucessCount = new AtomicInteger(0)

    wal.appendEntry(SimpleRaftLogEntry("hello1", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello2", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello3", 0))

    val f1 = tracker.trackRequest(0).onSuccess(_ => sucessCount.incrementAndGet())
    val f2 = tracker.trackRequest(1).onSuccess(_ => sucessCount.incrementAndGet())
    val f3 = tracker.trackRequest(2).onSuccess(_ => sucessCount.incrementAndGet())

    val request = AppendEntriesRequest(0, "process-0",
      None, entries, 0)

    val successResponse1 = AppendEntriesResponse(0, false, "process-1")
    val successResponse2 = AppendEntriesResponse(0, false, "process-2")

    when(raftClients("process-1").appendEntries(request)).thenReturn(Future(successResponse1))
    when(raftClients("process-2").appendEntries(request)).thenReturn(Future(successResponse2))

    tracker.moveCommitIndexOnce()
    // reduce because leader should decrement index and try again.
    tracker.nextIndices should be (Map(
      "process-1" -> 0L,
      "process-2" -> 0L,
    ))
    tracker.matchIndices should be (Map(
      "process-1" -> 0L,
      "process-2" -> 0L,
    ))
    raftStateImpl.commitIndex should be (0)
    a [TimeoutException] should be thrownBy {
      Await.result(Future.join(f1, f2, f3), Duration.fromMilliseconds(0))
    }
    sucessCount.get() should be(0)
  }

  "Tracker" should "correctly add to an empty when there is less than quorum network failures" in {
    val entries = Seq(SimpleRaftLogEntry("hello1", 0), SimpleRaftLogEntry("hello2", 0),
      SimpleRaftLogEntry("hello3", 0))
    val sucessCount = new AtomicInteger(0)

    wal.appendEntry(SimpleRaftLogEntry("hello1", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello2", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello3", 0))

    val f1 = tracker.trackRequest(0).onSuccess(_ => sucessCount.incrementAndGet())
    val f2 = tracker.trackRequest(1).onSuccess(_ => sucessCount.incrementAndGet())
    val f3 = tracker.trackRequest(2).onSuccess(_ => sucessCount.incrementAndGet())

    val request = AppendEntriesRequest(0, "process-0",
      None, entries, 0)

    val successResponse1 = AppendEntriesResponse(0, true, "process-1")

    when(raftClients("process-1").appendEntries(request)).thenReturn(Future(successResponse1))
    when(raftClients("process-2").appendEntries(request)).thenReturn(Future.const(Throw(new RuntimeException)))
    tracker.moveCommitIndexOnce()
    tracker.nextIndices should be (Map(
      "process-1" -> 4L,
      "process-2" -> 1L,
    ))
    tracker.matchIndices should be (Map(
      "process-1" -> 3L,
      "process-2" -> 0L,
    ))
    raftStateImpl.commitIndex should be (3)
    Await.result(Future.join(f1, f2, f3), Duration.fromMilliseconds(0))
    sucessCount.get() should be(3)
  }

  "Tracker" should "not finish future when replication fails due to network error" in {
    val entries = Seq(SimpleRaftLogEntry("hello1", 0), SimpleRaftLogEntry("hello2", 0),
      SimpleRaftLogEntry("hello3", 0))
    val sucessCount = new AtomicInteger(0)

    wal.appendEntry(SimpleRaftLogEntry("hello1", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello2", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello3", 0))

    val f1 = tracker.trackRequest(0).onSuccess(_ => sucessCount.incrementAndGet())
    val f2 = tracker.trackRequest(1).onSuccess(_ => sucessCount.incrementAndGet())
    val f3 = tracker.trackRequest(2).onSuccess(_ => sucessCount.incrementAndGet())

    val request = AppendEntriesRequest(0, "process-0",
      None, entries, 0)

    when(raftClients("process-1").appendEntries(request)).thenReturn(Future.const(Throw(new RuntimeException)))
    when(raftClients("process-2").appendEntries(request)).thenReturn(Future.const(Throw(new RuntimeException)))
    tracker.moveCommitIndexOnce()
    tracker.nextIndices should be (Map(
      "process-1" -> 1L,
      "process-2" -> 1L,
    ))
    tracker.matchIndices should be (Map(
      "process-1" -> 0L,
      "process-2" -> 0L,
    ))
    raftStateImpl.commitIndex should be (0)
    a [TimeoutException] should be thrownBy {
      Await.result(Future.join(f1, f2, f3), Duration.fromMilliseconds(0))
    }
    sucessCount.get() should be(0)
  }

  "Tracker" should "move commit index smallest possible value" in {
    val entries = Seq(SimpleRaftLogEntry("hello1", 0), SimpleRaftLogEntry("hello2", 0),
      SimpleRaftLogEntry("hello3", 0))
    val sucessCount = new AtomicInteger(0)

    wal.appendEntry(SimpleRaftLogEntry("hello1", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello2", 0))
    wal.appendEntry(SimpleRaftLogEntry("hello3", 0))

    val f1 = tracker.trackRequest(0).onSuccess(_ => sucessCount.incrementAndGet())
    val f2 = tracker.trackRequest(1).onSuccess(_ => sucessCount.incrementAndGet())
    val f3 = tracker.trackRequest(2).onSuccess(_ => sucessCount.incrementAndGet())

    val request = AppendEntriesRequest(0, "process-0",
      None, entries, 0)

    val successResponse1 = AppendEntriesResponse(0, true, "process-1")
    val successResponse2 = AppendEntriesResponse(0, true, "process-2")

    when(raftClients("process-1").appendEntries(request)).thenReturn(Future(successResponse1))
    when(raftClients("process-2").appendEntries(request)).thenReturn(Future(successResponse2))
    tracker.moveCommitIndexOnce()
    raftStateImpl.commitIndex should be (3)
    Await.result(Future.join(f1, f2, f3))
    sucessCount.get() should be(3)
    tracker.inFlightRequests.isEmpty shouldBe(true)
  }

  "Tracker" should "handle different nextIndices and matchIndices for the followers" in {
    hydrateTracker(50)

    val entries = Seq(SimpleRaftLogEntry("hello51", 0))
    val sucessCount = new AtomicInteger(0)

    wal.appendEntry(SimpleRaftLogEntry("hello51", 0))

    val f1 = tracker.trackRequest(51).onSuccess(_ => sucessCount.incrementAndGet())

    val request = AppendEntriesRequest(0, "process-0",
      Some(RaftLogMetadata(50, 0)), entries, 50)

    val successResponse2 = AppendEntriesResponse(0, true, "process-2")

    when(raftClients("process-1").appendEntries(request)).thenReturn(Future.const(Throw(new RuntimeException)))
    when(raftClients("process-2").appendEntries(request)).thenReturn(Future(successResponse2))
    tracker.moveCommitIndexOnce()

    tracker.nextIndices should be (Map(
      "process-1" -> 51L,
      "process-2" -> 52L,
    ))
    tracker.matchIndices should be (Map(
      "process-1" -> 50L,
      "process-2" -> 51L,
    ))
    raftStateImpl.commitIndex should be (51)
    Await.result(f1)
    tracker.inFlightRequests.isEmpty shouldBe(true)
  }

  "Tracker" should "hydrate the WAL with 100 entries" in {
    hydrateTracker(100)

    tracker.nextIndices should be (Map(
      "process-1" -> 101L,
      "process-2" -> 101L,
    ))
    tracker.matchIndices should be (Map(
      "process-1" -> 100L,
      "process-2" -> 100L,
    ))
  }

  def hydrateTracker(number: Int): Unit = {
    var prevLogMeta: Option[RaftLogMetadata] = None
    (1 to number).foreach { i =>
      val entry = SimpleRaftLogEntry(s"hello$i", 0)
      val index = wal.appendEntry(entry)
      val f = tracker.trackRequest(index)

      val request = AppendEntriesRequest(0, "process-0", prevLogMeta, Seq(entry), raftStateImpl.commitIndex)
      val successResponse1 = AppendEntriesResponse(0, true, "process-1")
      val successResponse2 = AppendEntriesResponse(0, true, "process-2")

      when(raftClients("process-1").appendEntries(request)).thenReturn(Future(successResponse1))
      when(raftClients("process-2").appendEntries(request)).thenReturn(Future(successResponse2))

      tracker.moveCommitIndexOnce()
      tracker.nextIndices should be (Map[String, Long](
        "process-1" -> (i + 1).toLong,
        "process-2" -> (i + 1).toLong,
      ))
      tracker.matchIndices should be (Map[String, Long](
        "process-1" -> i.toLong,
        "process-2" -> i.toLong,
      ))

      Await.result(f)
      prevLogMeta = Some(RaftLogMetadata(i, 0))
      tracker.inFlightRequests.isEmpty shouldBe(true)
    }
  }
}
