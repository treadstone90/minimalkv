package com.treadstone90.mkvstore.raft

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.eventbus.EventBus
import com.twitter.util.{Duration, Time, Timer, TimerTask}

class FollowerActor(eventBus: EventBus,
                    timer: Timer,
                    raftState: RaftState,
                    raftClients: Map[String, RaftClient]) extends Actor {

  var currentTimerTask: Option[TimerTask] = None
  val isLocked = new AtomicBoolean(false)

  def timeoutTask(): TimerTask = {
    timer.schedule(Time.now.plus(Duration.fromSeconds(10))) {
      if(isLocked.compareAndSet(false, true)) {
        //  here I need to transition into a candidate state
        eventBus.post(TransitionStateRequest(Candidate))
        currentTimerTask = Some(timeoutTask())
        isLocked.set(false)
      }
    }
  }

  def handleAppendEntriesRequest(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse = {
    while(!isLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())
    val response: AppendEntriesResponse = if(appendEntriesRequest.heartBeatEvent.nonEmpty) {
      val heartbeatEvent = appendEntriesRequest.heartBeatEvent.get
      println(s"Received heartbeat from $heartbeatEvent")

      // get a monotonically increasing term.
      raftState.monotonicUpdate(heartbeatEvent.leaderTerm) match {
        case None => AppendEntriesResponse(heartbeatEvent.leaderTerm, true)
        case Some(greaterTerm) => AppendEntriesResponse(greaterTerm, fals)
      }
      val currentTerm = raftState.currentTerm()
      if(currentTerm > heartbeatEvent.leaderTerm) {
        AppendEntriesResponse(currentTerm, false)
      } else {
        // set value to leader's term.
        if(currentTerm < heartbeatEvent.leaderTerm) {
          raftState.withExclusiveLock {
            raftState.setCurrentTerm(heartbeatEvent.leaderTerm)
          }
        }
        AppendEntriesResponse(currentTerm, true)
      }



    } else {
      println("received a non heart beat event.")
      AppendEntriesResponse(-1, true)
    }
    isLocked.set(false)
    timeoutTask()
    response
  }

  def handleRequestVoteRequest(requestVoteRequest: RequestVoteRequest): RequestVoteResponse = {
    raftState.withExclusiveLock {
      // if it is greater it is only going to get greater.
      val currentTerm = raftState.currentTerm()
      if(currentTerm > requestVoteRequest.term) {
        RequestVoteResponse(currentTerm, false)
      } else {
        // currentTerm is lesser but it can keep on increasing.
        raftState.setCurrentTerm(requestVoteRequest.term)
        if(raftState.voteIfEmpty()) {
          RequestVoteResponse(requestVoteRequest.term, true)
        } else {
          // follower aloready voted for another candidate in this term.
          RequestVoteResponse(requestVoteRequest.term, false)
        }
        val voted = raftState.votedFor(requestVoteRequest.term)
        if(voted.isEmpty || voted.contains(requestVoteRequest.candidateId)) {
          raftState.setCurrentTerm(requestVoteRequest.term)
          raftState.voteFor(requestVoteRequest.term, requestVoteRequest.candidateId)
          RequestVoteResponse(currentTerm, true)
        } else {
          RequestVoteResponse(currentTerm, false)
        }



      }
    }
  }

  override def shutDown(): Unit = {
    while(!isLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())
  }

  override def startUp(): Unit = {
    currentTimerTask = Some(timeoutTask())
  }
}

case class RaftLogEntry(logIndex: Long, logTerm: Int)