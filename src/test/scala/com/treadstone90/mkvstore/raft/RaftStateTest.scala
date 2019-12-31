package com.treadstone90.mkvstore.raft

import java.io.File
import java.nio.file.{Files, Paths}

import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class RaftStateTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach {
  var raftStateImpl: RaftState = _
  var tempFile = File.createTempFile("raftmeta", "test").getAbsolutePath

  override def beforeEach(): Unit = {
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
  }

  override def afterEach(): Unit = {
    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    Files.delete(Paths.get(tempFile))
  }

  "RaftState" should "increment term from empty" in {
    raftStateImpl.incrementTerm() should be (0L)
    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()

    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(0L))
  }

  "RaftState" should "increment Term from any other state" in {
    raftStateImpl.incrementTerm() should be (0L)
    raftStateImpl.incrementTerm() should be (1L)
    val result = (0 to 100).par.map(_ => raftStateImpl.incrementTerm()).toList
    (2 to 102).toList should be (result.sorted)

    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(102L))
  }

  "RaftState" should "monotonic update if request is larger" in {
    raftStateImpl.incrementTerm() should be (0L)
    raftStateImpl.incrementTerm() should be (1L)
    raftStateImpl.monotonicUpdate(2, Some("host004")) should be (true, Some((2, Some("host004"))))

    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(2L))
    raftStateImpl.currentCandidate should be (Some("host004"))
  }

  "RaftState" should "not monotonic update if request is smaller" in {
    raftStateImpl.incrementTerm() should be (0L)
    raftStateImpl.incrementTerm() should be (1L)
    raftStateImpl.monotonicUpdate(0, Some("host004")) should be (false, Some((1, Some("host001"))))

    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(1L))
    raftStateImpl.currentCandidate should be (Some("host001"))
  }

  "RaftState" should "not monotonic update if request is same but host is different" in {
    raftStateImpl.incrementTerm() should be (0L)
    raftStateImpl.incrementTerm() should be (1L)
    raftStateImpl.monotonicUpdate(1, Some("host004")) should be (false, Some((1, Some("host001"))))


    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(1L))
    raftStateImpl.currentCandidate should be (Some("host001"))
  }

  "RaftState" should "monotonic update if not term is set" in {
    raftStateImpl.monotonicUpdate(3, Some("host004")) should be (true, Some((3,Some("host004"))))

    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(3L))
    raftStateImpl.currentCandidate should be (Some("host004"))
  }

  "RaftState" should "monotonic update without a candidate vote" in {
    raftStateImpl.incrementTerm() should be (0L)
    raftStateImpl.incrementTerm() should be (1L)
    raftStateImpl.monotonicUpdate(2, None) should be (true, Some((2, None)))

    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(2L))
    raftStateImpl.currentCandidate should be (None)
  }

  "RaftState" should "monotonic update if request is larger - parallel version" in {
    raftStateImpl.incrementTerm() should be (0L)
    raftStateImpl.incrementTerm() should be (1L)
    (0 to 100).par.foreach { i =>
      raftStateImpl.monotonicUpdate(i, Some(s"host$i"))
    }

    raftStateImpl.currentTerm should be (Some(100))
    raftStateImpl.currentCandidate should be (Some("host100"))


    (0 to 50).par.foreach { i =>
      raftStateImpl.monotonicUpdate(i, Some(s"host$i")) should be (false, Some((100, Some("host100"))))
    }


    raftStateImpl.stopAsync()
    raftStateImpl.awaitTerminated()
    // read back and verify the state.
    raftStateImpl = new RaftStateImpl(3, "host001", tempFile)
    raftStateImpl.startAsync()
    raftStateImpl.awaitRunning()
    raftStateImpl.currentTerm should be (Some(100L))
    raftStateImpl.currentCandidate should be (Some("host100"))
  }
}
