package com.treadstone90.mkvstore.raft


import com.google.common.eventbus.EventBus
import com.twitter.conversions.DurationOps._
import com.twitter.util.{MockTimer, Time}
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers._
import org.scalatest.flatspec.AnyFlatSpec

class FollowerActorTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach  {
  var raftState: RaftState = _
  var raftClients: Map[String, RaftClient] = _
  var eventBus: EventBus = _
  var timer: MockTimer = _
  var followerActor: FollowerActor = _

  override def beforeEach(): Unit = {
    raftState = mock[RaftState]
    eventBus = mock[EventBus]
    raftClients = Map.empty[String, RaftClient]
    (1 to 3).foreach { candidateNum =>
      val candidateId = s"candidate$candidateNum"
      raftClients.updated(candidateId, mock[RaftClient])
    }
  }

  "FollowerActor" should "transition to Candidate on timer expiry" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(11.seconds)
      timer.tick()
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)
    }
  }

  it should "reset timer on receipt on a heart beat event and handle currentTerm < leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(8.seconds)
      timer.tick()

      // t = 8 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)

      // t = 8 get a heartbeat
      val heartBeat = HeartBeatEvent(2, "leader001", Time.now.inMilliseconds)
      when(raftState.currentTerm()).thenReturn(1)
      followerActor.handleAppendEntriesRequest(AppendEntriesRequest(None, Some(heartBeat))) should be (AppendEntriesResponse(1, true))
      verify(raftState, times(1)).setCurrentTerm(2)

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
      followerActor.isLocked.get() should be (false)
    }
  }

  it should "reset timer on receipt on a heart beat event and handle currentTerm == leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(8.seconds)
      timer.tick()

      // t = 8 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)

      // t = 8 get a heartbeat
      val heartBeat = HeartBeatEvent(1, "leader001", Time.now.inMilliseconds)
      when(raftState.currentTerm()).thenReturn(1)
      followerActor.handleAppendEntriesRequest(AppendEntriesRequest(None, Some(heartBeat))) should be (AppendEntriesResponse(1, true))

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
      followerActor.isLocked.get() should be (false)
    }
  }

  it should "reset timer on receipt on a heart beat event and handle currentTerm > leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(8.seconds)
      timer.tick()

      // t = 8 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)

      // t = 8 get a heartbeat
      val heartBeat = HeartBeatEvent(1, "leader001", Time.now.inMilliseconds)
      when(raftState.currentTerm()).thenReturn(2)
      followerActor.handleAppendEntriesRequest(AppendEntriesRequest(None, Some(heartBeat))) should be (AppendEntriesResponse(2, false))
      verify(raftState, never).setCurrentTerm(anyInt)

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
      followerActor.isLocked.get() should be (false)
    }
  }

  it should "handle RequestVoteRPC and not grant vote when currentTerm > leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)

      // t = 5
      val voteRequest = RequestVoteRequest(1, "candidate001", 0, 1)

      when(raftState.currentTerm()).thenReturn(2)
      when[RequestVoteResponse](raftState.withExclusiveLock[RequestVoteResponse](any())).thenAnswer[RequestVoteResponse]((f: RequestVoteResponse) => f)
      followerActor.handleRequestVoteRequest(voteRequest) should be (RequestVoteResponse(2, false))

      // t = 12 verify that a new timer was set
      timeControl.advance(7.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)
    }
  }

  it should "handle RequestVoteRPC and grant vote when currentTerm < leaderTerm " +
    "and no vote is granted for term and log ???" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)

      when(raftState.currentTerm()).thenReturn(1)
      when(raftState.votedFor(2)).thenReturn(None)
      when[RequestVoteResponse](raftState.withExclusiveLock[RequestVoteResponse](any())).thenAnswer[RequestVoteResponse]((f: RequestVoteResponse) => f)
      followerActor.handleRequestVoteRequest(voteRequest) should be (RequestVoteResponse(1, true))

      // t = 12 verify that a new timer was set
      timeControl.advance(7.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be (false)
    }
  }

  it should "handle RequestVoteRPC and grant vote when currentTerm < leaderTerm " +
    "and vote is granted for term to request candidate" in {

    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be(false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)

      when(raftState.currentTerm()).thenReturn(1)
      when(raftState.votedFor(2)).thenReturn(Some("candidate001"))
      when[RequestVoteResponse](raftState.withExclusiveLock[RequestVoteResponse](any())).thenAnswer[RequestVoteResponse]((f: RequestVoteResponse) => f)
      followerActor.handleRequestVoteRequest(voteRequest) should be(RequestVoteResponse(1, true))

      // t = 12 verify that a new timer was set
      timeControl.advance(7.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be(false)
    }
  }

  it should "handle RequestVoteRPC and not grant vote when currentTerm < leaderTerm " +
    "and vote is granted for term to another candidate" in {

    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be(false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)

      when(raftState.currentTerm()).thenReturn(1)
      when(raftState.votedFor(2)).thenReturn(Some("candidate002"))
      when[RequestVoteResponse](raftState.withExclusiveLock[RequestVoteResponse](any())).thenAnswer[RequestVoteResponse]((f: RequestVoteResponse) => f)
      followerActor.handleRequestVoteRequest(voteRequest) should be(RequestVoteResponse(1, false))

      // t = 12 verify that a new timer was set
      timeControl.advance(7.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be(false)
    }
  }

  it should "handle RequestVoteRPC when currentTerm == leaderTerm" in {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      followerActor = new FollowerActor(eventBus, timer, raftState, raftClients)
      followerActor.start()
      timeControl.advance(5.seconds)
      timer.tick()

      // t = 5 nothing happens
      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, never).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be(false)

      // t = 5
      val voteRequest = RequestVoteRequest(2, "candidate001", 0, 1)

      when(raftState.currentTerm()).thenReturn(2)
      when(raftState.votedFor(2)).thenReturn(Some("candidate001"))
      when[RequestVoteResponse](raftState.withExclusiveLock[RequestVoteResponse](any())).thenAnswer[RequestVoteResponse]((f: RequestVoteResponse) => f)
      followerActor.handleRequestVoteRequest(voteRequest) should be(RequestVoteResponse(2, true))

      // t = 12 verify that a new timer was set
      timeControl.advance(7.seconds)
      timer.tick()

      assert(followerActor.currentTimerTask.isDefined)
      verify(eventBus, times(1)).post(TransitionStateRequest(Candidate))
      followerActor.isLocked.get() should be(false)
    }
  }
}
