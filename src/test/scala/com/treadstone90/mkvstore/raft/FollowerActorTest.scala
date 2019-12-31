package com.treadstone90.mkvstore.raft


import com.google.common.eventbus.EventBus
import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.conversions.DurationOps._
import com.twitter.util.{MockTimer, Time}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class FollowerActorTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach  {
  var raftState: RaftState = _
  var raftClients: Map[String, RaftClient] = _
  var eventBus: EventBus = _
  var followerActor: FollowerActor = _
  var writeAheadLog: WriteAheadLog[RaftLogEntry] = _
  val followerSettings = FollowerSettings(10000)

  override def beforeEach(): Unit = {
    raftState = mock[RaftState]
    when(raftState.processId).thenReturn("local001")
    eventBus = mock[EventBus]
    raftClients = Map.empty[String, RaftClient]
    (1 to 3).foreach { candidateNum =>
      val candidateId = s"candidate$candidateNum"
      raftClients.updated(candidateId, mock[RaftClient])
    }
    writeAheadLog = new MemoryWAL
  }


  "FollowerActor" should "transition to Candidate on timer expiry" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(11.seconds)
      timer.tick()
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)
      
      followerActor.stop()
    }
  }

  it should "reset timer on receipt on a heart beat event and handle currentTerm < leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(8.seconds)
      timer.tick()

      // t = 8 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)

      // t = 8 get a heartbeat
      val heartBeat = AppendEntriesRequest(2, "leader001", None, Seq.empty, 0)
      when(raftState.monotonicUpdate(2, None)).thenReturn((true, None))
      followerActor.handleAppendEntriesRequest(heartBeat) should be (AppendEntriesResponse(2, true, "local001"))

      // t = 16 verify that old timer did not fire and send a state transition request
      timeControl.advance(8.seconds)
      timer.tick()
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.currentTimerTask.nonEmpty shouldBe true
      // verify that the correct timer was set by advancing time.

      // t = 20 verify that a new timer was set
      timeControl.advance(4.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)
      
      followerActor.stop()
    }
  }

  it should "reset timer on receipt on a heart beat event and handle currentTerm == leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(8.seconds)
      timer.tick()

      // t = 8 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)

      // t = 8 get a heartbeat
      val heartBeat = AppendEntriesRequest(1, "leader001", None, Seq.empty, 0)
      when(raftState.monotonicUpdate(1, None)).thenReturn((true, None))
      followerActor.handleAppendEntriesRequest(heartBeat) should be (AppendEntriesResponse(1, true, "local001"))

      // t = 16 verify that old timer did not fire and send a state transition request
      timeControl.advance(8.seconds)
      timer.tick()
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.currentTimerTask.nonEmpty shouldBe true
      // verify that the correct timer was set by advancing time.

      // t = 20 verify that a new timer was set
      timeControl.advance(4.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)
      
      followerActor.stop()
    }
  }

  it should "reset timer on receipt on a heart beat event and handle currentTerm > leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(8.seconds)
      timer.tick()

      // t = 8 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)

      // t = 8 get a heartbeat
      val heartBeat = AppendEntriesRequest(1, "leader001", None, Seq.empty, 0)
      when(raftState.monotonicUpdate(1, None)).thenReturn((false, Some(2, Some("leader003"))))
      followerActor.handleAppendEntriesRequest(heartBeat) should be (AppendEntriesResponse(2, false, "local001"))

      // t = 16 verify that old timer did not fire and send a state transition request
      timeControl.advance(8.seconds)
      timer.tick()
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.currentTimerTask.nonEmpty shouldBe true
      // verify that the correct timer was set by advancing time.

      // t = 20 verify that a new timer was set
      timeControl.advance(4.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)
      
      followerActor.stop()
    }
  }

  it should "handle RequestVoteRPC and not grant vote when currentTerm > leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)

      // t = 5
      val voteRequest = RequestVoteRequest(1, "candidate001", 0, 1)
      when(raftState.monotonicUpdate(1, Some("candidate001"))).thenReturn((false, Some(2, Some("leader003"))))
      // reset timer to fire now +  10
      followerActor.handleRequestVoteRequest(voteRequest) should be (RequestVoteResponse(2, false))

      // t = 12 verify that a new timer was set
      timeControl.advance(12.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)
      
      followerActor.stop()
    }
  }

  it should "handle RequestVoteRPC and grant vote when currentTerm < leaderTerm " +
    "and no vote is granted for term and log ???" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)
      when(raftState.monotonicUpdate(2, Some("candidate001"))).thenReturn((true, None))
      followerActor.handleRequestVoteRequest(voteRequest) should be (RequestVoteResponse(2, true))

      // t = 12 verify that a new timer was set
      timeControl.advance(12.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be (false)
      
      followerActor.stop()
    }
  }

  it should "handle RequestVoteRPC and grant vote when currentTerm < leaderTerm " +
    "and vote is granted for term to request candidate" in {

    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be(false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)

      when(raftState.monotonicUpdate(voteRequest.term, Some(voteRequest.candidateId))).thenReturn((true, None))
      followerActor.handleRequestVoteRequest(voteRequest) should be(RequestVoteResponse(2, true))

      // t = 12 verify that a new timer was set
      timeControl.advance(12.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be(false)
      
      followerActor.stop()
    }
  }


  // This is a weird test
  it should "handle RequestVoteRPC and not grant vote when currentTerm < leaderTerm " +
    "and vote is granted for term to another candidate" in {

    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be(false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)

      when(raftState.monotonicUpdate(voteRequest.term, Some(voteRequest.candidateId))).thenReturn((false, Some((2, Some("candidate002")))))
      followerActor.handleRequestVoteRequest(voteRequest) should be(RequestVoteResponse(2, false))

      // t = 12 verify that a new timer was set
      timeControl.advance(12.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be(false)
      
      followerActor.stop()
    }
  }

  it should "handle RequestVoteRPC when currentTerm == leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, followerSettings, writeAheadLog, raftState)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be(false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)

      when(raftState.monotonicUpdate(voteRequest.term, Some(voteRequest.candidateId))).thenReturn((true, None))
      followerActor.handleRequestVoteRequest(voteRequest) should be(RequestVoteResponse(2, true))

      // t = 12 verify that a new timer was set
      timeControl.advance(12.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isTimerLocked.get() should be(false)
      
      followerActor.stop()
    }
  }

  it should "apply entries to log prevLog is empty" in {

  }

  it should ""
}
