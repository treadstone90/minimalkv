package com.treadstone90.mkvstore.raft

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.AbstractIdleService
import com.treadstone90.mkvstore.WriteAheadLog
import com.twitter.util._

class CandidateActor(candidateId: String,
                     writeAheadLog: WriteAheadLog,
                     timer: Timer,
                     raftState: RaftState,
                     eventBus: EventBus,
                     raftClients: Map[String, RaftClient]) extends AbstractIdleService {

  var currentTimerTask: Option[TimerTask] = None
  val isLocked = new AtomicBoolean(false)

  override def startUp(): Unit = {
    currentTimerTask = initiateElection()
  }

  def initiateElection(): Option[TimerTask] = {
    val currentTerm = raftState.incrementCurrentTerm()
    raftState.voteFor(currentTerm, candidateId)
    val requestVoteRequest = RequestVoteRequest(currentTerm, candidateId, -1, -1)
    val requestVoteResponse = raftClients.map(_._2.requestVote(requestVoteRequest))
    val responses = Await.result(Future.collect(requestVoteResponse.toSeq))
    val count = responses.count(_.voteGranted)

    if (count >= raftState.quorum) {
      eventBus.post(TransitionStateRequest(Leader))
      None
    } else {
      Some(timeoutTask())
    }
  }

  def timeoutTask(): TimerTask = {
    timer.schedule(Time.now.plus(Duration.fromSeconds(10))) {
      if (!isLocked.compareAndSet(false, true)) {
        currentTimerTask = initiateElection()
        isLocked.set(false)
      }
    }
  }

  def handleAppendEntries(appendEntriesRequest: AppendEntriesRequest): AppendEntriesResponse = {
    while (!isLocked.compareAndSet(false, true)) {}
    currentTimerTask.foreach(_.cancel())
    val response: AppendEntriesResponse = if (appendEntriesRequest.heartBeatEvent.nonEmpty) {
      println(s"Received heartbeat from ${appendEntriesRequest.heartBeatEvent.get.leaderId}")
      val currentTerm = raftState.currentTerm()
      if (appendEntriesRequest.appendEntriesEvent.get.leaderTerm < currentTerm) {
        // needs to continue in the candidate state.
        AppendEntriesResponse(currentTerm, false)
      } else {
        val response = AppendEntriesResponse(currentTerm, true)
        // convert candidate to follower
        eventBus.post(TransitionStateRequest(Follower))
        response
      }
    } else {
      println("received a non heart beat event.")
      AppendEntriesResponse(-1, true)
    }
    isLocked.set(false)
    timeoutTask()
    response
  }

  def handleRequestVote(requestVoteRequest: RequestVoteRequest): RequestVoteResponse = {
    raftState.withExclusiveLock {
      val currentTerm = raftState.currentTerm()
      if (requestVoteRequest.term < currentTerm) {
        RequestVoteResponse(currentTerm, false)
      } else {
        val voted = raftState.votedFor(requestVoteRequest.term)
        if (voted.isEmpty || voted.contains(requestVoteRequest.candidateId)) {
          raftState.voteFor(requestVoteRequest.term, requestVoteRequest.candidateId)
          raftState.setCurrentTerm(requestVoteRequest.term)
          RequestVoteResponse(currentTerm, true)
        } else {
          RequestVoteResponse(currentTerm, false)
        }
      }
    }
  }

  def shutDown(): Unit = {
     currentTimerTask.foreach(_.cancel())
  }
}
